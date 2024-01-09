from datetime import datetime, timedelta
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from helper.mixpannel_helper import Mixpanel
from schema import mixpanel_schema
from utility.s3_utility import S3Helper

col_name_map = {
    "time": "sys_ts",
    # "timestamp": "api_ts",
    "role": "account_role",
    # "event": "class_event",
    "version": "release_version",
    "session": "api_session_id",
    "action": "sys_action",
    "type": "class_type",
}

spark = (
    SparkSession.builder.master("local[8]")
    .config("spark.driver.memory", "8G")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.3.3")
    .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    .getOrCreate()
)

s3 = S3Helper("vsx-warehouse-data")


def normalized_key(old_key: str) -> str:
    if old_key.startswith("$"):
        return old_key.replace("$", "")
    return old_key


def flat_mutli_layers(input: dict, prefix=None) -> dict:
    rs_dict = {}

    for k, v in input.items():
        k = normalized_key(k)
        if prefix:
            k = prefix + "." + k

        if isinstance(v, dict):
            rs_dict.update(flat_mutli_layers(v, k))
        elif isinstance(v, list):
            rs_dict[k] = ",".join(str(v))
        else:
            rs_dict[k] = v

        if k in col_name_map:
            rs_dict[col_name_map[k]] = rs_dict.pop(k)

    return rs_dict


def process_and_load(content: str, event: str):
    records = []
    for line in content.splitlines():
        event_dict = json.loads(line)
        flat_dict = flat_mutli_layers(event_dict["properties"])
        records.append(flat_dict)
    schema = mixpanel_schema.generate_schema_by_event(event)
    df = spark.read.json(spark.sparkContext.parallelize(records), schema=schema)
    now_time = datetime.now()
    df.select(lit(now_time).alias("create_date"))
    df.select(lit(now_time).alias("last_upd_date"))
    df.show()


def is_file_expired(file_path: str) -> bool:
    ts = os.path.splitext(os.path.basename(file_path))[0]
    file_ctime = datetime.strptime(ts, "%Y%m%d%H%M%S")
    now = datetime.now()
    thresh_time = file_ctime + timedelta(hours=1)
    return now > thresh_time


def find_recent_file(prefix: str) -> str:
    files = s3.list_object(prefix)
    if files:
        return max(files)
    return None  # type: ignore


def download_file(obj_name: str) -> str:
    content = s3.download_file_stream(obj_name)
    return content


def upload_file(obj_name: str, content: str):
    s3.upload_file_stream(content, obj_name)


if __name__ == "__main__":
    events = [
        "Disconnect",
        "LeaveClass",
        "LessonEnd",
        "LessonStart",
        "Login",
        "Logout",
        "Reconnect",
        "StudentJoin",
        "PushBtn",
        "ClassListClick",
        "QRCodeClick",
        "ClassLinkCopy",
        "QuizStart",
        "QuizEnd",
    ]
    events = ["LessonEnd"]
    # TODO: argument or fetch from DB
    sdate_str = "2023-12-27"

    mixpanel = Mixpanel(1)

    for event in events:
        print(f"Now process event {event}")

        s3_folder = f"dev/mixpanel/class_swift/{event.lower()}"
        recent_file = find_recent_file(s3_folder)
        is_expired = is_file_expired(recent_file)

        content = None
        if recent_file and not is_expired:
            print(f"Download file {recent_file}")
            content = download_file(recent_file)
        else:
            print("Sent a request")
            today = datetime.today()
            content = mixpanel.send_request(
                sdate_str,
                datetime.strftime(today, "%Y-%m-%d"),
                event=[event],
            )

            fout_name = f"{datetime.strftime(today, '%Y%m%d%H%M%S')}.json"
            upload_file(fout_name, content)

        if content:
            process_and_load(content, event.lower())
