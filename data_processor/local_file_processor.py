import glob
import os
from data_processor.base_file_processor import BaseFileProcessor


class LocalFileProcessor(BaseFileProcessor):
    def find_recent_file(self, folder_path: str) -> str:
        file_path = f"{folder_path}/*.json"
        files = glob.glob(file_path)
        if files:
            return max(files)
        return None  # type: ignore

    def download_file(self, full_file_path: str):
        print(f"read file {full_file_path}")
        content = None
        with open(full_file_path, "r") as fin:
            content = fin.read()
        return content

    def upload_file(self, full_file_path: str, content: str) -> bool:
        with open(full_file_path, "w") as fout:
            fout.write(content)
        return True

    def check_file_exist(self, full_file_path: str):
        return os.path.exists(full_file_path)
