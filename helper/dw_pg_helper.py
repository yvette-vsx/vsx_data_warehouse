from sqlalchemy import create_engine
from config import PG_DEV_CONNECT_URL

engine = create_engine(PG_DEV_CONNECT_URL, echo=True)