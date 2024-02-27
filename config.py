import os
from dotenv import load_dotenv

load_dotenv()

PG_WH_PROD_CONFIG = {
    "host": os.getenv("DW_PG_PROD_HOST"),
    "user": os.getenv("DW_PG_PROD_USER"),
    "password": os.getenv("DW_PG_PROD_PWD"),
    "port": os.getenv("DW_PG_PROD_PORT"),
}

PG_WH_DEV_CONFIG = {
    "host": os.getenv("DW_PG_DEV_HOST"),
    "user": os.getenv("DW_PG_DEV_USER"),
    "password": os.getenv("DW_PG_DEV_PWD"),
    "port": os.getenv("DW_PG_DEV_PORT"),
}
_PG_DW_CONNECT_URL = (
    "postgresql+psycopg2://%(user)s:%(password)s@%(host)s:%(port)s/warehouse"
)
PG_PROD_CONNECT_URL = _PG_DW_CONNECT_URL % PG_WH_PROD_CONFIG
PG_DEV_CONNECT_URL = _PG_DW_CONNECT_URL % PG_WH_DEV_CONFIG

MIX_PANEL_SVC_ACCOUNT_PROD = (
    os.getenv("MX_SVC_AC_PROD_USER"),
    os.getenv("MX_SVC_AC_PROD_SECRET"),
)
MIX_PANEL_CS_PROJECT_ID_PROD = os.getenv("MX_SVC_AC_PROD_PROJECT_ID")

MIX_PANEL_SVC_ACCOUNT_STG = (
    os.getenv("MX_SVC_AC_STAG_USER"),
    os.getenv("MX_SVC_AC_STAG_SECRET"),
)
MIX_PANEL_CS_PROJECT_ID_STG = os.getenv("MX_SVC_AC_STAG_PROJECT_ID")
