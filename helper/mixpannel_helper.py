import requests
import urllib.parse
from config import MIX_PANEL_CS_PROJECT_ID_PROD, MIX_PANEL_CS_PROJECT_ID_STG, MIX_PANEL_SVC_ACCOUNT_PROD, MIX_PANEL_SVC_ACCOUNT_STG
from utility.constants import EnviroType 

class Mixpanel:

    base_url = "https://data.mixpanel.com/api/2.0/export?"
    
    def __init__(self, envir=1):
        self.envir = envir

    def url_encode(self, paras:dict) -> str:
        new_url = urllib.parse.urlencode(paras)
        return new_url.replace("%27", "%22")

    def api_request(self, paras:dict) -> requests.Response:
        url = Mixpanel.base_url + self.url_encode(paras)
        svc_account = None
        if self.envir == EnviroType.ENVIRO_CODE_PROD:
            svc_account = MIX_PANEL_SVC_ACCOUNT_PROD
            project_id = MIX_PANEL_CS_PROJECT_ID_PROD
        else:
            svc_account = MIX_PANEL_SVC_ACCOUNT_STG
            project_id = MIX_PANEL_CS_PROJECT_ID_STG

        paras["project_id"] = project_id
        return requests.get(url, auth=svc_account)
    



