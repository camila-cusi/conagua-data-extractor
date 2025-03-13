# from pathlib import Path
# from typing import List, Dict
# import json


# class PathUtils():
#     def __init__(self):
#         pass

#     def get_list_files_in_directory(self, directory: Path) -> List[str]:
#         return [file.name for file in directory.iterdir() if file.is_file()]

#     def load_config(self) -> Dict:
#         """
#         Loads the configuration from the config.json file located in the same
#         directory as this module.
#         """
#         config_path = Path(__file__).parent / "config.json"
#         if not config_path.exists():
#             raise FileNotFoundError(f"Config file {config_path} not found.")
#         with open(config_path, "r") as f:
#             return json.load(f)
