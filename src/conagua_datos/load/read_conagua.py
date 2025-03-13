import pandas as pd
from pathlib import Path
from src.conagua_datos.extract.downloader import DownloadCONAGUA
from src.conagua_datos.transform.processor import DataProcessor
from src.conagua_datos.transform import filter_data
import logging
import warnings

logging.basicConfig(level=logging.INFO)
warnings.filterwarnings("ignore")


class ReadConaguaDatos:
    def __init__(
            self,
            start_date: str,
            end_date: str,
            directory_path: str = None,
            data_type: str = "lluvia",
            include_pronostico: bool = False
    ) -> None:
        logging.info(f"Downloading data for {data_type}")
        self.data_type = data_type
        self.start_date = start_date
        self.end_date = end_date
        self.directory = directory_path
        self.downloader = DownloadCONAGUA()
        self.processor = DataProcessor()
        self.data_filter = filter_data
        self.pronostico = include_pronostico

    def _read_conagua_datos(self) -> pd.DataFrame:
        # Download the data
        self.downloader.download_conagua(
            data_type=self.data_type,
            include_pronostico=self.pronostico
        )
        logging.info(f"Download for {self.data_type} successful!")
        # Process the downloaded data
        ca_df = self.processor.process_data(self.downloader.directory)
        # Filter the DataFrame based on dates
        filtered_df = self.data_filter.filtrar_datos(
            ca_df, start_date=self.start_date, end_date=self.end_date
        )
        return filtered_df

    def get_conagua_df(self) -> pd.DataFrame:
        return self._read_conagua_datos()

    def get_conagua_excel(self, file_location: str) -> None:
        df = self._read_conagua_datos()
        if file_location:
            output_path = Path(file_location)
        else:
            output_path = Path.cwd() / "conagua_data.xlsx"
        df.to_excel(output_path, index=False)

    def get_conagua_csv(self, file_location: str = None) -> None:
        df = self._read_conagua_datos()
        if file_location:
            output_path = Path(file_location)
        else:
            output_path = Path.cwd() / "conagua_data.csv"
        df.to_csv(output_path, index=False)
