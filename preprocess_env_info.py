from functools import lru_cache
import re
import pandas as pd
from sqlalchemy import create_engine
from config import PG_PROD_CONNECT_URL

env_os_regex = r"(?P<os>^(\w|\s)+)(?P<version>(\.\d+)+)"


@lru_cache(maxsize=None)
def fetch_country_mapping(index=None):
    engine = create_engine(PG_PROD_CONNECT_URL)
    sql = "select * from coutry_vs_region"
    if index:
        country_df = pd.read_sql(sql, con=engine, index_col=index)
    else:
        country_df = pd.read_sql(sql, con=engine)
    return country_df


def fetch_alpha2_and_vs_region(alpha3: str):
    country_df = fetch_country_mapping("alpha_3")
    tar_df = country_df.loc[alpha3]
    if isinstance(tar_df, pd.DataFrame):
        return (tar_df.iloc[0]["alpha_2"], tar_df.iloc[0]["vs_region"])
    elif isinstance(tar_df, pd.Series):
        return (tar_df["alpha_2"], tar_df["vs_region"])
    return None


def preprocess_env_info(raw_dict):
    trans_dict = {}
    if "os" in raw_dict:
        matcher = re.match(env_os_regex, raw_dict.get("os"))
        trans_dict["os"] = matcher["os"]
    if "screens" in raw_dict and (v := raw_dict["screens"]):
        temp = [v[0]]
        for i in range(1, len(v)):
            temp.append(v[i]["screen_height"])
            temp.append(v[i]["screen_width"])
        trans_dict["screen_height"] = temp[1]
        trans_dict["screen_width"] = temp[1]
        trans_dict["screens"] = ",".join(str(x) for x in temp)
    if "region" in raw_dict and (rcode := raw_dict["region"]):
        region_info = fetch_alpha2_and_vs_region(rcode)
        if region_info:
            trans_dict["country_code"] = region_info[0]
            trans_dict["vs_region"] = region_info[1]
    return trans_dict
