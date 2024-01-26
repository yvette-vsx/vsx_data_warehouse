import sys
import logging
from logging.handlers import TimedRotatingFileHandler
from utility.env_util import PROJECT_NAME, LOG_DIR, LOG_FILE

LOG_DIR.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger(PROJECT_NAME)
logger.setLevel(logging.INFO)

file_handler = TimedRotatingFileHandler(
    LOG_FILE, when="midnight", interval=1, backupCount=14
)

file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
    logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
)
file_handler.suffix = "%Y%m%d"

sys_handler = logging.StreamHandler(sys.stdout)
sys_handler.setLevel(logging.INFO)
sys_handler.setFormatter(
    logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
)

logger.addHandler(file_handler)
logger.addHandler(sys_handler)
