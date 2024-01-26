import os
import socket
from pathlib import Path


PROJECT_PATH = Path(__file__).parent.parent.absolute()
PROJECT_NAME = os.path.basename(Path(__file__).parent.parent.absolute())
LOG_DIR = Path(os.path.join(PROJECT_PATH, "log"))
LOG_FILE = os.path.join(LOG_DIR, f"{PROJECT_NAME}.log")
HOST_NAME = socket.gethostname()
HOST_IP = socket.gethostbyname(HOST_NAME)
HOME_DIR = os.environ.get("HOME")
CURRENT_USER = os.path.basename(Path(HOME_DIR))  # type: ignore

# print(PROJECT_PATH)
# print(PROJECT_NAME)
# print(LOG_DIR)
# print(LOG_FILE)
# print(HOST_NAME)
# print(HOST_IP)
# print(HOME_DIR)
# print(CURRENT_USER)
