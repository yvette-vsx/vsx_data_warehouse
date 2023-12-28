import configparser

config = configparser.ConfigParser()
config.read("config.ini")

PG_WH_PROD_CONFIG = {
    "host": config["dw.pg.prod"]["host"],
    "user": config["dw.pg.prod"]["user"],
    "password": config["dw.pg.prod"]["pwd"],
    "port": config["dw.pg.prod"]["port"],
}

PG_WH_DEV_CONFIG = {
    "host": config["dw.pg.dev"]["host"],
    "user": config["dw.pg.dev"]["user"],
    "password": config["dw.pg.dev"]["pwd"],
    "port": config["dw.pg.dev"]["port"],
}
_PG_DW_CONNECT_URL = (
    "postgresql+psycopg2://%(user)s:%(password)s@%(host)s:%(port)s/warehouse"
)
PG_PROD_CONNECT_URL = _PG_DW_CONNECT_URL % PG_WH_PROD_CONFIG
PG_DEV_CONNECT_URL = _PG_DW_CONNECT_URL % PG_WH_DEV_CONFIG

MIX_PANEL_SVC_ACCOUNT_PROD = (config["mixpanel.service.account.prod"]["user"], 
                           config["mixpanel.service.account.prod"]["secret"])
MIX_PANEL_CS_PROJECT_ID_PROD = config["mixpanel.service.account.prod"]["peojectid"]

MIX_PANEL_SVC_ACCOUNT_STG = (config["mixpanel.service.account.stg"]["user"], 
                           config["mixpanel.service.account.stg"]["secret"])
MIX_PANEL_CS_PROJECT_ID_STG = config["mixpanel.service.account.stg"]["peojectid"]



