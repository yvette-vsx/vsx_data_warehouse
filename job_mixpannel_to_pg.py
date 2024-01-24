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

tz_cst = ZoneInfo("Asia/Taipei")
tz_gmt = ZoneInfo("GMT")

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
            flat_dict[MixpanelColName.MP_TIMESTAMP.value], tz_cst
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
    now_time = datetime.now(tz=tz_cst)
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
    print(f"{sdate_dict['unix_start_date']}, epoch={sdate_dict['unix_start_epoch']}")
    return {
        "unix_start_date": datetime.strftime(sdate_dict["unix_start_date"], "%Y-%m-%d"),
        "unix_start_epoch": sdate_dict["unix_start_epoch"],
    }


def send_process(sdate_str, edate_str, event_list, fout_name, file_processor):
    print("Sent a request")
    content = mixpanel.send_request(sdate_str, edate_str, event=event_list)  # type: ignore
    upload: bool = file_processor.upload_file(fout_name, content)
    if upload:
        print(f"upload file to s3 {fout_name} successfully")
    return content


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-ev", "--events", help="ex:Disconnect,Reconnect")
    parser.add_argument(
        "-s", "--sdate", help="ex:20240105, default is max of mp_ts in DB"
    )
    parser.add_argument(
        "-e",
        "--edate",
        help="ex:20240105, it and sdate could be same day. default is today",
    )
    # --file 參數加入難以判斷日期重複的事件，變成要先手動確認DB資料狀況，故先拿掉
    # parser.add_argument("-f", "--file", help="20240117183816.json")
    parser.add_argument("--test", help="store file in local FS", action="store_true")
    parser.add_argument(
        "--notcheck",
        help="not check the request has been sent in one hour",
        action="store_true",
    )
    args = parser.parse_args()

    events = [
        "Disconnect",
        "Reconnect",
        "LessonEnd",
        "LessonStart",
        "Login",
        "Logout",
        "LeaveClass",
        "StudentJoin",
        "PushBtn",
        "QuizStart",
        "QuizEnd",
    ]
    if args.events:
        events = args.events.split(",")

    today: datetime = datetime.now(tz=tz_cst)
    del_time_dict = None
    sdate_str = None
    edate_str = None

    if args.sdate:
        sdate_str = args.sdate
        edate_str = args.edate

        del_time_dict = fetch_unix_startdate_by_date(
            int(sdate_str[:4]), int(sdate_str[4:6]), int(sdate_str[6:])
        )

    file_processor = None
    root_path = None
    env_enum = EnviroType.PROD
    if args.test:
        env_enum = EnviroType.DEV
        root_path = "./data/mixpanel"
        from data_processor.local_file_processor import LocalFileProcessor

        file_processor = LocalFileProcessor()

    else:
        root_path = f"{env_enum.name.lower()}/mixpanel/class_swift"
        from data_processor.aws_file_processor import AWSFileProcess

        file_processor = AWSFileProcess()

    mixpanel = Mixpanel(env_enum)

    for e in events:
        # event [LeaveClass] was renamed [StudentLeave]
        if e == "LeaveClass":
            event_list = [e, "StudentLeave"]
            event = "studentleave"
        else:
            event_list = [e]
            event = e.lower()
        print(f"Now process event [{event}]")

        table_name = f"mp_{event}"
        folder_path = f"{root_path}/{event}"

        if not args.sdate:
            del_time_dict = find_max_unix_date_in_wh(table_name)
            sdate_str = del_time_dict["unix_start_date"]
            edate_str = datetime.strftime(datetime.now(tz=tz_gmt), "%Y-%m-%d")

        # epoch=32503680000 ->  3000/1/1
        del_epoch = del_time_dict.get("unix_start_epoch", 32503680000)

        print(f"request data from {sdate_str} to {edate_str}")
        cnt = ph.delete_by_epoch_time(table_name, del_epoch)
        print(
            f"delete [{cnt} records] in DW table [{ph.schema}.{table_name}] where [mp_ts >= {del_epoch}]"
        )

        if args.notcheck:
            fout_name = f"{root_path}/backfile/{event}/{sdate_str}_{edate_str}.json"
            content = send_process(
                sdate_str, edate_str, event_list, fout_name, file_processor
            )
        else:
            recent_file = file_processor.find_recent_file(folder_path)
            is_expired = is_file_expired(recent_file) if recent_file else False
            if recent_file and not is_expired:
                print(f"download file:{recent_file}")
                content = file_processor.download_file(recent_file)
            else:
                fout_name = (
                    f"{folder_path}/{datetime.strftime(today, '%Y%m%d%H%M%S')}.json"
                )
                content = send_process(
                    sdate_str, edate_str, event_list, fout_name, file_processor
                )
        ### 指定檔案，寫入DB時可能會造成data duplication, 故不納入自動化流程，手動執行時要注意會刪掉>=start_date之後的資料
        # content = file_processor.download_file(
        #     "prod/mixpanel/class_swift/backfile/studentleave/20240115_20240123.json"
        # )
        if content:
            load(process_transform(content), event)
