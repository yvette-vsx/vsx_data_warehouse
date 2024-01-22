from datetime import datetime, timedelta
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when
from zoneinfo import ZoneInfo


from config import PG_WH_PROD_CONFIG
from helper.mixpannel_helper import Mixpanel
from helper import pg_mixpanel_helper as ph
from schema import mixpanel_schema
from utility.constants import (
    EnviroType,
    MixpanelColName,
    DWCommonColName,
    MixpanelEvent,
)
from utility.s3_utility import S3Helper


col_name_map = {
    "time": MixpanelColName.MP_TIMESTAMP.value,
    "role": MixpanelColName.CS_ROLE.value,
    "version": MixpanelColName.CS_VERSION_ID.value,
    "client": MixpanelColName.CS_CLIENT.value,
    "user_id": MixpanelColName.CS_USER_ID.value,
    "class_id": MixpanelColName.CS_ROOM_ID.value,
}

spark = (
    SparkSession.builder.master("local[4]")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.3.3")
    .getOrCreate()
)

s3 = S3Helper("vsx-warehouse-data")
cst_tz = ZoneInfo("Asia/Taipei")

eliminate_head_sign = lambda input: input.replace("$", "")
add_prefix = lambda prefix, input: prefix + "." + input


def normalized_key(old_key: str, prefix=None) -> str:
    if prefix:
        return add_prefix(prefix, eliminate_head_sign(old_key))
    return eliminate_head_sign(old_key)


def flat_mutli_layers(input: dict, prefix=None) -> dict:
    rs_dict = {}

    for k, v in input.items():
        k = normalized_key(k, prefix)
        # Renamed the key name, if the value is a dict(nested). The k remained.
        map_k = col_name_map.get(k, k)
        if isinstance(v, dict):
            rs_dict.update(flat_mutli_layers(v, k))
        elif isinstance(v, list):
            rs_dict[map_k] = ",".join(str(v))
        else:
            rs_dict[map_k] = v
    return rs_dict


def process_transform(content: str):
    records = []
    schema = mixpanel_schema.generate_schema_by_event(event)
    fields = schema.jsonValue()["fields"]
    notnull_cols = [field["name"] for field in fields if not field["nullable"]]
    for line in content.splitlines():
        event_dict = json.loads(line)
        flat_dict = flat_mutli_layers(event_dict["properties"])
        # Added a column mp_td from mp_ts
        flat_dict[MixpanelColName.MP_DT.value] = datetime.fromtimestamp(
            flat_dict[MixpanelColName.MP_TIMESTAMP.value], cst_tz
        )
        if check_data_not_nullable(flat_dict, notnull_cols):
            records.append(flat_dict)
        else:
            print(flat_dict)
    return records


def check_data_not_nullable(record, constrained_cols):
    is_valid = True
    for name in constrained_cols:
        value = record.get(name)
        if not value:
            is_valid = False
            break
    return is_valid


def load(records: list[dict], event: str):
    schema = mixpanel_schema.generate_schema_by_event(event)
    df = spark.createDataFrame(records, schema=schema)
    now_time = datetime.now(tz=cst_tz)
    rs_df = df.withColumn(
        DWCommonColName.DW_CREATE_DATE.value, lit(now_time)
    ).withColumn(DWCommonColName.DW_LAST_UPD_DATE.value, lit(now_time))

    if event in (MixpanelEvent.RECONNECT.value, MixpanelEvent.DISCONNECT.value):
        cs_user_id_name = MixpanelColName.CS_USER_ID.value
        rs_df = rs_df.withColumn(
            cs_user_id_name, when((df.cs_user_id.isNull()), df.distinct_id)
        )

    url = "jdbc:postgresql://{host}:{port}/warehouse".format(
        host=PG_WH_PROD_CONFIG["host"], port=PG_WH_PROD_CONFIG["port"]
    )
    properties = {
        "user": PG_WH_PROD_CONFIG["user"],
        "password": PG_WH_PROD_CONFIG["password"],
        "driver": "org.postgresql.Driver",
    }
    rs_df.write.jdbc(url, f"mixpanel.mp_{event}", mode="append", properties=properties)


def is_file_expired(file_path: str) -> bool:
    ts = os.path.splitext(os.path.basename(file_path))[0]
    file_ctime = datetime.strptime(ts, "%Y%m%d%H%M%S")
    file_ctime.tzinfo
    now = datetime.now(tz=cst_tz)
    thresh_time = file_ctime + timedelta(hours=1)
    now_aware = thresh_time.replace(tzinfo=cst_tz)
    return now > now_aware


def find_recent_file(prefix: str) -> str:
    files = s3.list_object(prefix)
    if files:
        return max(files)
    return None  # type: ignore


def download_file(obj_name: str) -> str:
    content = s3.download_file_stream(obj_name)
    return content


def upload_file(obj_name: str, content: str):
    return s3.upload_file_stream(content, obj_name)


# def download_file(file_path: str):
#     print(f"read file {recent_file}")
#     content = None
#     with open(recent_file, "r") as fin:
#         content = fin.read()
#     return content


# def upload_file(file_path: str, content: str) -> bool:
#     with open(file_path, "w") as fout:
#         fout.write(content)
#     return True


# def find_recent_file(path: str) -> str:
#     import glob

#     file_path = f"{path}/*.json"
#     files = glob.glob(file_path)
#     if files:
#         return max(files)
#     return None  # type: ignore


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
    print(f"{sdate_dict['unix_start_date']}, epoch={sdate_dict['unix_start_epoch']}")
    return {
        "unix_start_date": datetime.strftime(sdate_dict["unix_start_date"], "%Y-%m-%d"),
        "unix_start_epoch": sdate_dict["unix_start_epoch"],
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--event", action="store_true")
    parser.add_argument(
        "--type",
        type=int,
        help="1=cusotmer; 2=trasaction; 3=product; 4=data_event; 5=action_log; 6=product_notify",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="only transaction was affected by this para",
    )
    args = parser.parse_args()

    events = [
        # "Disconnect",
        # "Reconnect",
        # "LessonEnd",
        # "LessonStart",
        # "Login",
        # "Logout",
        # "LeaveClass",
        # "StudentJoin",
        # "PushBtn",
        # "QuizStart",
        # "QuizEnd",
    ]
    today = datetime.now(tz=cst_tz)
    # TODO: argument or fetch from DB
    sdate_str = "2024-01-01"
    edate_str = "2024-01-06"
    # edate_str = datetime.strftime(today, "%Y-%m-%d")

    mixpanel = Mixpanel(EnviroType.PROD)

    for event in events:
        table_name = event.lower()
        print(f"Now process event {event}")

        s3_folder = f"{EnviroType.PROD.name.lower()}/mixpanel/class_swift/{table_name}"
        # s3_folder = f"./data/mixpanel/{event}"
        recent_file = find_recent_file(s3_folder)
        # is_expired = is_file_expired(recent_file) if recent_file else False
        is_expired = False
        content = None
        if recent_file and not is_expired:
            print(f"Download file {recent_file}")
            content = download_file(recent_file)
        else:
            print("Sent a request")
            content = mixpanel.send_request(sdate_str, edate_str, event=[event])
            fout_name = f"{s3_folder}/{datetime.strftime(today, '%Y%m%d%H%M%S')}.json"
            upload: bool = upload_file(fout_name, content)
            if upload:
                print(f"upload file to s3 {fout_name} successfully")

        if content:
            if event == "LeaveClass":
                table_name = "studentleave"
            load(process_transform(content), table_name)
