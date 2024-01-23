import abc

class BaseFileProcessor:

    @abc.abstractmethod
    def find_recent_file(self, folder_path: str) -> str:
        return NotImplemented

    @abc.abstractmethod
    def download_file(self, full_file_path: str) -> str:
        return NotImplemented

    @abc.abstractmethod
    def upload_file(self, full_file_path: str, content: str):
        return NotImplemented
    
        