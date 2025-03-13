import pandas as pd
from typing import Dict, List
import re
from unidecode import unidecode
from pathlib import Path
from src.conagua_datos.utils import PathUtils


class DataProcessor():
    def __init__(self):
        self.path_utils = PathUtils()

    def get_dataframes_files_path(
            self,
            working_directory_path: Path,
            list_files: List[str]
    ) -> Dict[str, pd.DataFrame]:
        """
        Obtain dictionary of dataframes from the files in the working
        directory path.
        """
        dict_of_files = {}
        for file in list_files:
            path = working_directory_path.joinpath(file)
            try:
                if file.endswith('.xls'):
                    dict_of_files[file] = pd.read_excel(path, engine='xlrd')
                elif file.endswith('.xlsx'):
                    dict_of_files[file] = pd.read_excel(
                        path, engine='openpyxl'
                    )
            except Exception as e:
                print(f"Error processing {file}: {e}")
        return dict_of_files

    def _clean_data(
            self,
            climate_dict: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        # edge_case_rain = (
        #     "PRECIPITACIÓN A NIVEL NACIONAL Y POR ENTIDAD "
        #     "FEDERATIVA"
        # )
        # edge_case_temp = (
        #     "TEMPERATURA MEDIA PROMEDIO A NIVEL NACIONAL Y POR "
        #     "ENTIDAD FEDERATIVA"
        # )

        edge_case_rain = self.path_utils.load_config()["edge_case_rain"]
        edge_case_temp = self.path_utils.load_config()["edge_case_temp"]

        for year in climate_dict.keys():
            lower_cols = climate_dict[year].columns.str.lower().str.strip()
            year_int = int(re.search(r'\d{4}', year).group(0))

            # Edge case
            # if (
            #     edge_case_rain in climate_dict[year].columns
            # ) or (edge_case_temp in climate_dict[year].columns):
            if (
                edge_case_rain in lower_cols
            ) or (
                edge_case_temp in lower_cols
            ):
                climate_dict[year].columns = climate_dict[year].iloc[0]
                climate_dict[year] = climate_dict[year].iloc[1:]
                climate_dict[year].rename(
                    columns={"ENTIDAD": 'estado'},
                    inplace=True
                )
                climate_dict[year].reset_index(drop=True, inplace=True)

            climate_dict[year] = self._make_cols_lower(climate_dict[year])
            climate_dict[year] = self._make_info_lower(
                climate_dict[year], ["estado"]
            )
            climate_dict[year].columns = climate_dict[year].columns.map({
                "estado": "estado", "anual": "anual",
                'ene': 1, 'feb': 2,
                'mar': 3, 'abr': 4,
                'may': 5, 'jun': 6,
                'jul': 7, 'ago': 8,
                'sep': 9, 'oct': 10,
                'nov': 11, 'dic': 12
            })
            climate_dict[year].drop(columns=["anual"], inplace=True)
            climate_dict[year] = climate_dict[year][
                climate_dict[year]["estado"] != "nacional"
            ]
            climate_dict[year] = climate_dict[year].T
            climate_dict[year].columns = climate_dict[year].iloc[0]
            climate_dict[year] = climate_dict[year].iloc[1:].reset_index(
                names="month"
            )
            climate_dict[year]["year"] = year_int
            # create date from month and year
            climate_dict[year].index = pd.to_datetime(
                climate_dict[year][['year', 'month']].assign(day=1)
            )
        return pd.concat(climate_dict.values()).rename_axis("date", axis=1)

    def _make_info_lower(
        self,
        df: pd.DataFrame,
        columns: list[str]
    ) -> pd.DataFrame:
        for col in columns:
            df[col] = df[col].apply(str).apply(
                unidecode
            ).str.strip().str.lower()
        return df

    def _make_cols_lower(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [
            unidecode(str(col).strip().lower()) for col in df.columns
        ]
        return df

    def process_data(
            self,
            directory: Path,
            order_cols: bool = True
    ) -> Dict[str, pd.DataFrame]:
        # Obtain files from directory
        files_list = self.path_utils.get_list_files_in_directory(directory)
        # Create dictionary of files with pd df
        df_dict = self.get_dataframes_files_path(
            working_directory_path=directory, list_files=files_list
        )
        # clean data
        process_df = self._clean_data(df_dict).sort_index()
        if order_cols:
            order_cols = self.path_utils.load_config()["order_col"]
            return process_df[order_cols]
        else:
            return process_df

# edge_case_rain = 'PRECIPITACIÓN A NIVEL NACIONAL Y POR ENTIDAD FEDERATIVA'
# edge_case_temp = 'TEMPERATURA MEDIA PROMEDIO A NIVEL NACIONAL Y POR ENTIDAD
# FEDERATIVA'
