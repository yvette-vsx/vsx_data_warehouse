from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def add_must_have_cols():
    return [
        StructField("mp_ts", IntegerType(), False),
        StructField("distinct_id", StringType(), False),
        StructField("insert_id", StringType(), False),
        StructField("cs_user_id", StringType(), True),
        StructField("cs_version", StringType(), True),
    ]


def add_client_must_have_cols():
    return [
        StructField("platform", StringType(), True),
        StructField("device", StringType(), True),
        StructField("screen_height", IntegerType(), True),
        StructField("screen_width", IntegerType(), True),
        StructField("cs_client", StringType(), True),
    ]


def add_other(col_name: str, data_type=StringType(), nullable=True):
    return StructField(col_name, data_type, nullable)


def add_room_id(nullable=True):
    return add_other("room_id")


def add_lesson_id(nullable=True):
    return add_other("lesson_id")


def add_device(nullable=True):
    return add_other("device")


def add_role(nullable=True):
    return add_other("cs_role")


def generate_schema_by_event(event: str):
    fields = add_must_have_cols()

    if event == "disconnect":
        fields.append(add_room_id())
        fields.append(add_lesson_id())
        fields.append(add_role())

    elif event == "studentleave":
        fields += add_client_must_have_cols()
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_other("browser"))
        fields.append(add_other("browser_version"))
        fields.append(add_other("city"))
        fields.append(add_other("device_id"))
        fields.append(add_other("os"))
        fields.append(add_other("region"))
        fields.append(add_other("mp_country_code"))
        fields.append(add_other("trigger_type"))
        fields.append(add_device())
        fields.append(add_other("referrer"))

    elif event == "lessonend":
        fields += add_client_must_have_cols()
        fields.append(add_room_id())
        fields.append(add_lesson_id())
        fields.append(add_device())

    elif event == "lessonstart":
        fields += add_client_must_have_cols()
        fields.append(add_room_id())
        fields.append(add_lesson_id())
        fields.append(add_device())

    elif event == "login":
        fields += add_client_must_have_cols()
        fields.append(add_device())

    elif event == "logout":
        fields += add_client_must_have_cols()
        fields.append(add_device())
        fields.append(add_room_id())

    elif event == "reconnect":
        fields.append(add_room_id())
        fields.append(add_lesson_id())
        fields.append(add_role())

    elif event == "studentjoin":
        fields += add_client_must_have_cols()
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_other("browser"))
        fields.append(add_other("browser_version"))
        fields.append(add_other("city"))
        fields.append(add_other("device_id"))
        fields.append(add_other("os"))
        fields.append(add_other("region"))
        fields.append(add_other("mp_country_code"))
        fields.append(add_other("trigger_type"))
        fields.append(add_device())
        fields.append(add_other("referrer"))

    return StructType(fields)
