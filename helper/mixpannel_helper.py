from datetime import datetime
import requests
import urllib.parse
from config import (
    MIX_PANEL_CS_PROJECT_ID_PROD,
    MIX_PANEL_CS_PROJECT_ID_STG,
    MIX_PANEL_SVC_ACCOUNT_PROD,
    MIX_PANEL_SVC_ACCOUNT_STG,
)
from utility.constants import EnviroType
from utility import datetime_utility as du


class Mixpanel:
    base_url = "https://data.mixpanel.com/api/2.0/export?"

    def __init__(self, envir=EnviroType.STAGE):
        self.envir: EnviroType = envir
        print(f"executing enviro is {envir.name}")

    def url_encode(self, paras: dict) -> str:
        new_url = urllib.parse.urlencode(paras)
        return new_url.replace("%27", "%22")

    def _get_project_id(self) -> str:
        if self.envir.value == EnviroType.PROD.value:
            return MIX_PANEL_CS_PROJECT_ID_PROD
        else:
            return MIX_PANEL_CS_PROJECT_ID_STG

    def _request(self, paras: dict) -> requests.Response:
        url = Mixpanel.base_url + self.url_encode(paras)
        svc_account = None
        if self.envir.value == EnviroType.PROD.value:
            svc_account = MIX_PANEL_SVC_ACCOUNT_PROD
        else:
            svc_account = MIX_PANEL_SVC_ACCOUNT_STG

        return requests.get(url, auth=svc_account)

    def send_request(self, sdate_str: str, edate_str: str, **kwargs) -> str:
        edate: datetime = datetime.strptime(edate_str.replace("-", ""), "%Y%m%d")
        if edate > datetime.today():
            edate_str = datetime.strftime(datetime.today(), "%Y-%m-%d")

        paras = {
            "project_id": self._get_project_id(),
            "from_date": du.transform_date_str_with_dash(sdate_str),
            "to_date": du.transform_date_str_with_dash(edate_str),
        }
        if kwargs:
            paras.update(kwargs)

        response = self._request(paras)
        content = response.text
        return content
