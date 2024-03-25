from datetime import datetime, timedelta
from functools import lru_cache
import json
import os
from pyspark.sql.functions import lit, when
from zoneinfo import ZoneInfo


from config import PG_WH_PROD_CONFIG
from helper.mixpannel_helper import Mixpanel
from helper import pg_mixpanel_helper as ph
from preprocess_env_info import preprocess_env_info
from schema import mixpanel_schema
from utility.constants import (
    EnviroType,
    MxpCol,
    CommonCol,
    MxpEvent,
)
from utility import spark_util
from utility.logger import logger

col_name_map = {
    "time": MxpCol.MP_TIMESTAMP.value,
    "role": MxpCol.CS_ROLE.value,
    "version": MxpCol.CS_VERSION_ID.value,
    "client": MxpCol.CS_CLIENT.value,
    "user_id": MxpCol.CS_USER_ID.value,
    "class_id": MxpCol.CS_ROOM_ID.value,
    "session": MxpCol.CS_SESSION.value,
}

tz_cst = ZoneInfo("Asia/Taipei")
tz_gmt = ZoneInfo("GMT")

eliminate_head_sign = lambda input: input.replace("$", "")
add_prefix = lambda prefix, input: prefix + "." + input
spark = spark_util.get_spark()


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
            rs_dict[map_k] = ",".join(str(x) for x in v)
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
        raw_properties = event_dict["properties"]
        raw_properties.update(preprocess_env_info(raw_properties))
        flat_dict = flat_mutli_layers(event_dict["properties"])
        # Added a column mp_td from mp_ts
        flat_dict[MxpCol.MP_DT.value] = datetime.fromtimestamp(
            flat_dict[MxpCol.MP_TIMESTAMP.value], tz_cst
        )
        if check_data_not_nullable(flat_dict, notnull_cols):
            records.append(flat_dict)
        else:
            logger.info(flat_dict)
    return records


def check_data_not_nullable(record, constrained_cols):
    is_valid = True
    for name in constrained_cols:
        value = record.get(name)
        if not value:
            is_valid = False
            break
    return is_valid


def load(records: list[dict], event: str, table_name: str):
    schema = mixpanel_schema.generate_schema_by_event(event)
    df = spark.createDataFrame(records, schema=schema)
    now_time = datetime.now(tz=tz_cst)
    rs_df = df.withColumn(CommonCol.DW_CREATE_DATE.value, lit(now_time)).withColumn(
        CommonCol.DW_LAST_UPD_DATE.value, lit(now_time)
    )

    if event in (MxpEvent.RECONNECT.value, MxpEvent.DISCONNECT.value):
        cs_user_id_name = MxpCol.CS_USER_ID.value
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
    rs_df.write.jdbc(
        url, f"mixpanel.{table_name}", mode="append", properties=properties
    )


def is_file_expired(file_path: str) -> bool:
    ts = os.path.splitext(os.path.basename(file_path))[0]
    file_ctime = datetime.strptime(ts, "%Y%m%d%H%M%S")
    file_ctime.tzinfo
    now = datetime.now(tz=tz_cst)
    thresh_time = file_ctime + timedelta(hours=1)
    now_aware = thresh_time.replace(tzinfo=tz_cst)
    return now > now_aware


def convert_unix_o_clock(year: int, month: int, day: int):
    """
    return a unix datetime object with time is 00:00:00 and its epoch time
    """
    o_clock_date = datetime(year, month, day, 0, 0, tzinfo=tz_gmt)
    return (o_clock_date, int(o_clock_date.timestamp()))


def convert_unix_o_clock_by_dw(table_name: str):
    last_epoch = ph.query_max_epoch_time(table_name)
    last_dt = datetime.fromtimestamp(last_epoch, tz_gmt)
    return convert_unix_o_clock(last_dt.year, last_dt.month, last_dt.day)


def send_process(sdate_str, edate_str, event_list, fout_name, file_processor):
    logger.info("Sent a request")
    content = mixpanel.send_request(sdate_str, edate_str, event=event_list)  # type: ignore
    upload: bool = file_processor.upload_file(fout_name, content)
    if upload:
        logger.info(f"upload file to s3 {fout_name} successfully")
    return content


@lru_cache(maxsize=None)
def gen_exec_time_info(input_start, input_end):
    o_clock_info = convert_unix_o_clock(
        int(input_start[:4]), int(input_start[4:6]), int(input_start[6:])
    )
    exec_time_dict = {
        "start_date": input_start,  # 20240313
        "end_date": input_end,  # 20240314
        "last_max_epoch": o_clock_info[1],  # int:1703140509
    }
    return exec_time_dict


def gen_exec_time_info_by_event(table_name):
    o_clock_info = convert_unix_o_clock_by_dw(table_name)
    exec_time_dict = {
        "start_date": datetime.strftime(o_clock_info[0], "%Y%m%d"),
        "end_date": datetime.strftime(datetime.now(tz=tz_gmt), "%Y%m%d"),
        "last_max_epoch": o_clock_info[1],
    }
    return exec_time_dict


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
        "EnvironmentInfo",
    ]
    if args.events:
        events = args.events.split(",")

    today: datetime = datetime.now(tz=tz_cst)
    file_processor = None
    env_enum = EnviroType.PROD

    if args.test:
        env_enum = EnviroType.DEV
        from data_processor.local_file_processor import LocalFileProcessor

        file_processor = LocalFileProcessor()

    else:
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

        table_name = f"mp_{event}"
        folder_path = f"{file_processor.path}/{event}"
        exec_time_info = (
            gen_exec_time_info(args.sdate, args.edate)
            if args.sdate
            else gen_exec_time_info_by_event(table_name)
        )
        sdate_str = exec_time_info.get("start_date")
        edate_str = exec_time_info.get("end_date")
        logger.info(f"Now process event [{event}], from {sdate_str} to {edate_str}")

        content = None
        if args.notcheck:
            fout_name = (
                f"{file_processor.path}/backfile/{event}/{sdate_str}_{edate_str}.json"
            )
            content = send_process(
                sdate_str, edate_str, event_list, fout_name, file_processor
            )
        else:
            recent_file = file_processor.find_recent_file(folder_path)
            is_expired = is_file_expired(recent_file) if recent_file else False
            if recent_file and not is_expired:
                logger.info(f"download file:{recent_file}")
                content = file_processor.download_file(recent_file)
            else:
                fout_name = (
                    f"{folder_path}/{datetime.strftime(today, '%Y%m%d%H%M%S')}.json"
                )
                content = send_process(
                    sdate_str, edate_str, event_list, fout_name, file_processor
                )
        ## 指定檔案，寫入DB時可能會造成data duplication, 故不納入自動化流程，手動執行時要注意會刪掉>=start_date之後的資料
        # content = file_processor.download_file(
        #     f"{file_processor.path}/backfile/{event}/{sdate_str}_{edate_str}.json"
        # )
        if content:
            # epoch=32503680000 ->  3000/1/1
            del_epoch = exec_time_info.get("last_max_epoch", 32503680000)
            cnt = ph.delete_by_epoch_time(table_name, del_epoch)
            logger.info(
                f"delete [{cnt} records] in DW table [{ph.schema}.{table_name}] where [mp_ts >= {del_epoch}]"
            )
            load(process_transform(content), event, table_name)
