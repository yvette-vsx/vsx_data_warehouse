from data_processor.base_file_processor import BaseFileProcessor
from utility.s3_utility import S3Helper


class AWSFileProcess(BaseFileProcessor):
    def __init__(self, bucket="vsx-warehouse-data"):
        self.s3 = S3Helper(bucket, "dw-s3")

    def find_recent_file(self, folder_path: str) -> str:
        files = self.s3.list_object(folder_path)
        if files:
            return max(files)
        return None  # type: ignore

    def download_file(self, full_file_path: str) -> str:
        print(f"download file from s3 {full_file_path}")
        content = self.s3.download_file_stream(full_file_path)
        return content

    def upload_file(self, full_file_path: str, content: str):
        return self.s3.upload_file_stream(content, full_file_path)

    def check_file_exist(self, full_file_path: str):
        return True if self.s3.get_object_meta(full_file_path) else False
