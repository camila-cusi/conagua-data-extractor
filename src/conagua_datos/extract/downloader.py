import os
import io
import zipfile
import requests
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple
import pandas as pd
from unidecode import unidecode
import tabula
from src.conagua_datos.utils import PathUtils
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

# Global month mapping constant (if needed outside the class)
month_map = {
    'enero': "ene",
    'febrero': "feb",
    'marzo': "mar",
    'abril': "abr",
    "mayo": "may",
    "junio": "jun",
    "julio": "jul",
    "agosto": "ago",
    "septiembre": "sep",
    "octubre": "oct",
    "noviembre": "nov",
    "diciembre": "dic"
}

logging.basicConfig(level=logging.INFO)


class DownloadCONAGUA:

    def __init__(self) -> None:
        # Optionally, store the month_map as an instance attribute.
        self.month_map = month_map
        self.directory = ""
        config_file = PathUtils().load_config()
        self.rain_url = config_file["rain_url"]
        self.temp_url = config_file["temp_url"]
        self.new_info_url = config_file['new_info_url']
        self.pronostic_url = config_file["pronostic_url"]

    def read_pdf_table(self, url: str) -> pd.DataFrame:
        """Reads the first table from the PDF at the given URL."""
        df = tabula.read_pdf(url, pages=1, lattice=True)[0]
        df.columns = [col.strip().lower() for col in df.columns]
        return df

    def _get_datatype_config(self, data_type: str) -> Dict[str, str]:
        """
        Returns a configuration dictionary for the given data type.
        The dictionary contains:
         - zip_url: URL for the ZIP file
         - pdf_folder: Folder path (relative) for the pronostic PDFs
         - default_folder: Local folder name to store downloaded data
         - filename: Default filename for saving new info
        """
        dt = data_type.lower()
        if dt in {"precipitacion", "rain", "lluvia"}:
            return {
                "zip_url": self.rain_url,
                "pdf_folder": "PREC",
                "default_folder": "Precipitacion",
                "filename": "Precip.xlsx"
            }
        elif dt in {"temperatura", "temp", "temperature"}:
            return {
                "zip_url": self.temp_url,
                "pdf_folder": "TMED",
                "default_folder": "Temperatura",
                "filename": "Tmed.xlsx"
            }
        else:
            raise ValueError(f"Invalid data type: {data_type}")

    def _get_datatype_values(
        self, data_type: str = "precipitacion"
    ) -> Tuple[str, Dict[int, pd.DataFrame], str, str]:
        """
        Obtains the ZIP URL, new information dictionary, default folder name,
        and filename based on the data type.
        new_info_dict maps each year (from 2024 to current year) to the
        corresponding DataFrame from its PDF.
        """
        latest_year = datetime.now().year
        config = self._get_datatype_config(data_type)
        zip_url = config["zip_url"]
        default_folder = config["default_folder"]
        pdf_folder = config["pdf_folder"]
        filename = config["filename"]

        new_info_dict = {
            year: self.read_pdf_table(
                f"{self.new_info_url}{pdf_folder}/{year}.pdf"
            )
            for year in range(2024, latest_year + 1)
        }
        return zip_url, new_info_dict, default_folder, filename

    def set_destination_directory(
            self, file_location: str = None, default_folder: str = ""
    ) -> Tuple[Path, Path]:
        """
        Determines the destination directory based on a provided file location
        or the default folder.
        Returns a tuple of (destination directory, path for temporary ZIP
        file).
        """
        if file_location is None:
            dest_dir = Path.cwd() / default_folder
        else:
            dest_dir = Path(file_location)
        dest_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Using destination directory: {dest_dir}")
        zip_file_path = dest_dir / "temp.zip"
        return dest_dir, zip_file_path

    def _download_zip_file(self, url: str, zip_file_path: Path) -> None:
        """
        Downloads a ZIP file from the specified URL and writes it to
        zip_file_path in chunks.
        """
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Failed to download file: {e}")
            raise

        with open(zip_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)
        logging.info(f"Downloaded zip file: {zip_file_path}")

    def _extract_zip_file(self, zip_file_path: Path, dest_dir: Path) -> None:
        """
        Extracts the contents of the ZIP file into the destination directory.
        If a common top-level folder exists, it is stripped from the
        extraction.
        """
        try:
            with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                all_names = zip_ref.namelist()
                common_prefix = os.path.commonprefix(all_names)
                if common_prefix and not common_prefix.endswith('/'):
                    common_prefix = common_prefix.split('/')[0] + '/'

                for member in zip_ref.infolist():
                    member_name = member.filename
                    if common_prefix and member_name.startswith(common_prefix):
                        member_name = member_name[len(common_prefix):]
                    target_path = dest_dir / member_name
                    if member.is_dir():
                        target_path.mkdir(parents=True, exist_ok=True)
                    else:
                        target_path.parent.mkdir(parents=True, exist_ok=True)
                        with zip_ref.open(member) as source, open(
                            target_path, "wb"
                        ) as target_file:
                            shutil.copyfileobj(source, target_file)
            logging.info(f"Extracted files into: {dest_dir}")
        except zipfile.BadZipFile as e:
            logging.error(f"Error extracting zip file: {e}")
            raise

    def add_pronostico(
            self, new_info_dict: Dict[int, pd.DataFrame]
    ) -> Dict[int, pd.DataFrame]:
        """
        Updates the new information dictionary (for the latest year) with
        pronostic data.
        The pronostic data is downloaded and merged into the DataFrame using
        month_map.
        """
        year_df = list(new_info_dict.keys())[-1]
        pronostic_dfs = self.get_latest_valid_pronostic()
        for key in pronostic_dfs.keys():
            # Assuming key.split("_")[2] returns the full month name
            key_month = self.month_map.get(key.split("_")[2])
            new_info_dict[year_df][key_month] = pronostic_dfs[
                key
            ]["pronostico (mm)"]
        return new_info_dict

    def _save_new_info(
            self,
            new_info_dict: Dict[int, pd.DataFrame],
            directory: Path,
            filename: str
    ) -> None:
        """
        Saves the new information DataFrames to the destination directory.
        Each file is saved as <year>_<filename>, e.g. '2025_Precip.xlsx'.
        """
        for key, df in new_info_dict.items():
            full_path = directory / f"{key}{filename}"
            df.to_excel(full_path, index=False)
            logging.info(f"Saved new info to: {full_path}")

    def download_conagua(
            self,
            file_location: str = None,
            data_type: str = "precipitacion",
            include_pronostico: bool = True
    ) -> None:
        """
        High-level method to download and extract CONAGUA data:
         - Determines the URL and new information based on data_type.
         - Sets the destination directory.
         - Downloads the ZIP file.
         - Extracts its contents.
         - Optionally, updates with pronostic data.
         - Saves the new information in the destination folder.
        """
        dtype_vales = self._get_datatype_values(data_type)
        url, new_info_dict, default_folder, filename = dtype_vales
        dest_dir, zip_file_path = self.set_destination_directory(
            file_location, default_folder
        )
        self._download_zip_file(url, zip_file_path)
        self._extract_zip_file(zip_file_path, dest_dir)
        if include_pronostico and (default_folder == "Precipitacion"):
            new_info_dict = self.add_pronostico(new_info_dict)
        self._save_new_info(new_info_dict, dest_dir, filename)
        zip_file_path.unlink()  # Clean up the temporary ZIP file
        self.directory = dest_dir

    def _generate_pronostic_url(
            self, year: int, month: int, tipo: str = "Lluvia"
    ) -> str:
        """
        Generates the URL for the climatic forecast ZIP for a given year and
        month.
        """
        month_names = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        month_prefix = {
            1: "01-EFM", 2: "02-FMA", 3: "03-MAM",
            4: "04-AMJ", 5: "05-MJJ", 6: "06-JJA",
            7: "07-JAS", 8: "08-ASO", 9: "09-SON",
            10: "10-OND", 11: "11-NDE", 12: "12-DEF"
        }
        # base_url = self.pronostic_url
        prefix = month_prefix.get(month)
        month_name = month_names.get(month)
        return (
            f"{self.pronostic_url}{prefix}-Pronostico-de-{month_name}-{year}-{tipo}.zip"
        )

    def download_extract_pronostic(self, url: str) -> Dict[str, pd.DataFrame]:
        """
        Downloads and extracts pronostic ZIP file data from the given URL.
        Returns a dictionary of DataFrames for CSV files located in the
        ESTADISTICAS folder.
        """
        response = requests.get(url)
        response.raise_for_status()
        dataframes = {}
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            estadistica_files = [
                f for f in z.namelist() if (
                    "ESTADISITCAS" in f.split("/")
                ) or (
                    "ESTADISTICAS" in f.split("/")
                )
            ]
            target_files = [
                file for file in estadistica_files
                if (
                    "csv" in file.split(".")
                ) and ("Estados" in file.split("_"))
            ]
            if not target_files:
                raise Exception("No target CSV files found in ZIP")
            for file in target_files:
                with z.open(file) as csvfile:
                    df = pd.read_csv(csvfile, encoding="latin-1")
                    df.rename(columns={"Unnamed: 0": "estado"}, inplace=True)
                    df.columns = [
                        unidecode(
                            str(col).strip().lower()
                        ) for col in df.columns
                    ]
                    dirname = os.path.dirname(file)
                    data_type = dirname.split("-")[-1].split("/")[0].lower()
                    basename_keys = os.path.basename(file).split("_")
                    key = f"{data_type}_{int(
                        basename_keys[3]
                    )}_{basename_keys[2].lower()}"
                    dataframes[key] = df.drop(
                        columns="cv_estado",
                        errors="ignore"
                    )
        return dataframes

    def get_latest_valid_pronostic(
            self, tipo: str = "Lluvia"
    ) -> Dict[str, pd.DataFrame]:
        """
        Attempts to download the pronostic ZIP for the current month.
        If not available, it decrements the month (adjusting the year as
        needed)
        until a valid file is found or after a maximum number of attempts.
        """
        now = datetime.now()
        year = now.year
        month = now.month
        attempts = 0
        max_attempts = 2  # Adjust as needed
        while attempts < max_attempts:
            url = self._generate_pronostic_url(year, month, tipo)
            logging.info(f"Trying URL: {url}")
            try:
                dfs = self.download_extract_pronostic(url)
                logging.info(
                    f"Successfully downloaded pronostic for {month}/{year}"
                )
                return dfs
            except (requests.HTTPError, zipfile.BadZipFile, Exception) as e:
                logging.warning(
                    f"Pronostic for {month}/{year} not available ({e}). "
                    f"Trying previous month."
                )
                month -= 1
                if month < 1:
                    month = 12
                    year -= 1
                attempts += 1
        raise Exception("No valid pronostic file found in the last 2 months.")
