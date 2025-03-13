import json
import inspect
from pathlib import Path
from typing import Dict, List


class PathUtils:
    def __init__(self):
        pass

    def get_list_files_in_directory(self, directory: Path) -> List[str]:
        return [file.name for file in directory.iterdir() if file.is_file()]

    def load_config(self) -> Dict:
        """
        Loads the configuration from the config.json file located in the same
        directory as the calling module.
        """
        # Get the caller's frame (one level up in the call stack)
        caller_frame = inspect.stack()[1]
        caller_module = inspect.getmodule(caller_frame[0])
        # Build the config file path relative to the caller's module location.
        caller_dir = Path(caller_module.__file__).parent
        config_path = caller_dir / "config.json"
        if not config_path.exists():
            raise FileNotFoundError(f"Config file {config_path} not found.")
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
