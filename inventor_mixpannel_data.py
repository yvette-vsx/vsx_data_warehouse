from datetime import datetime, timedelta
import json
import os
from zoneinfo import ZoneInfo
import pandas as pd
from sqlalchemy import create_engine

from config import PG_PROD_CONNECT_URL
from helper.mixpannel_helper import Mixpanel
from helper import pg_mixpanel_helper as ph
from utility.constants import EnviroType
from utility.logger import logger

tz_cst = ZoneInfo("Asia/Taipei")
tz_gmt = ZoneInfo("GMT")

eliminate_head_sign = lambda input: input.replace("$", "")
add_prefix = lambda prefix, input: prefix + "." + input
engine = create_engine(PG_PROD_CONNECT_URL)
col_name_map = {"class_id": "room_id"}


def normalized_key(old_key: str, prefix=None) -> str:
    if prefix:
        return add_prefix(prefix, eliminate_head_sign(old_key))
    return eliminate_head_sign(old_key)


def flat_mutli_layers(input: dict, prefix=None) -> dict:
    rs_dict = {}

    for k, v in input.items():
        k = normalized_key(k, prefix)
        map_k = col_name_map.get(k, k)
        # Renamed the key name, if the value is a dict(nested). The k remained.
        if isinstance(v, dict):
            rs_dict.update(flat_mutli_layers(v, k))
        elif isinstance(v, list):
            rs_dict[map_k] = ",".join(
                str(x).replace("\\", "").replace(".", "") for x in v
            )
        else:
            rs_dict[map_k] = v
    return rs_dict


def transform_load(content: str, table: str):
    records = []
    for line in content.splitlines():
        event_dict = json.loads(line)
        flat_dict = flat_mutli_layers(event_dict["properties"])
        records.append(flat_dict)
    df = pd.DataFrame(records)
    df.to_sql(table, engine, if_exists="append", index=False)


def is_file_expired(file_path: str) -> bool:
    ts = os.path.splitext(os.path.basename(file_path))[0]
    file_ctime = datetime.strptime(ts, "%Y%m%d%H%M%S")
    file_ctime.tzinfo
    now = datetime.now(tz=tz_cst)
    thresh_time = file_ctime + timedelta(hours=1)
    now_aware = thresh_time.replace(tzinfo=tz_cst)
    return now > now_aware


def fetch_unix_startdate_by_date(year: int, month: int, day: int):
    """
    return a unix datetime object with time is 00:00:00 and its epoch time
    """
    tz_gmt = ZoneInfo("GMT")
    startdate = datetime(year, month, day, 0, 0, tzinfo=tz_gmt)
    return {
        "unix_start_date": startdate,
        "unix_start_epoch": int(startdate.timestamp()),
    }


def find_max_unix_date_in_wh(table_name: str):
    last_epoch = ph.query_max_epoch_time(table_name)
    tz_gmt = ZoneInfo("GMT")
    last_dt = datetime.fromtimestamp(last_epoch, tz_gmt)
    sdate_dict = fetch_unix_startdate_by_date(last_dt.year, last_dt.month, last_dt.day)
    logger.info(
        f"{sdate_dict['unix_start_date']}, epoch={sdate_dict['unix_start_epoch']}"
    )
    return {
        "unix_start_date": datetime.strftime(sdate_dict["unix_start_date"], "%Y-%m-%d"),
        "unix_start_epoch": sdate_dict["unix_start_epoch"],
    }


def send_process(sdate_str, edate_str, event_list, fout_name, file_processor):
    logger.info("Sent a request")
    content = mixpanel.send_request(sdate_str, edate_str, event=event_list)  # type: ignore
    upload: bool = file_processor.upload_file(fout_name, content)
    if upload:
        logger.info(f"upload file {fout_name} successfully")
    return content


if __name__ == "__main__":

    events = ["EnvironmentInfo"]

    today: datetime = datetime.now(tz=tz_cst)
    del_time_dict = None

    sdate_str = "20240301"
    edate_str = "20240312"

    del_time_dict = fetch_unix_startdate_by_date(
        int(sdate_str[:4]), int(sdate_str[4:6]), int(sdate_str[6:])
    )

    env_enum = EnviroType.PROD
    from data_processor.local_file_processor import LocalFileProcessor

    file_processor = LocalFileProcessor()
    mixpanel = Mixpanel(env_enum)

    for e in events:

        event_list = [e]
        event = e.lower()
        logger.info(f"Now process event [{event}]")

        table_name = f"mp_{event}"
        folder_path = f"./data/mixpanel/{event}"
        not_check = False

        # if not_check:
        #     fout_name = f"./data/mixpanel/{event}/{sdate_str}_{edate_str}.json"
        #     content = send_process(
        #         sdate_str, edate_str, event_list, fout_name, file_processor
        #     )
        # else:
        #     recent_file = file_processor.find_recent_file(folder_path)
        #     is_expired = is_file_expired(recent_file) if recent_file else False
        #     if recent_file and not is_expired:
        #         logger.info(f"download file:{recent_file}")
        #         content = file_processor.download_file(recent_file)
        #     else:
        #         fout_name = (
        #             f"{folder_path}/{datetime.strftime(today, '%Y%m%d%H%M%S')}.json"
        #         )
        #         content = send_process(
        #             sdate_str, edate_str, event_list, fout_name, file_processor
        #         )

        # del_epoch = del_time_dict.get("unix_start_epoch", 32503680000)

        ### 指定檔案，寫入DB時可能會造成data duplication, 故不納入自動化流程，手動執行時要注意會刪掉>=start_date之後的資料
        content = file_processor.download_file(
            f"/Users/yuyv/py_projects/vsx_data_warehouse/data/mixpanel/EnvironmentInfo/20240301_20240312.json"
        )
        if content:
            # cnt = ph.delete_by_epoch_time(table_name, del_epoch)
            # logger.info(
            #     f"delete [{cnt} records] in DW table [{ph.schema}.{table_name}] where [mp_ts >= {del_epoch}]"
            # )
            transform_load(content, table_name)
