from pyspark.sql.types import (
    StructType,
    TimestampType,
    StructField,
    StringType,
    IntegerType,
)
from utility.constants import MixpanelColName, MixpanelEvent


def add_must_have_cols():
    return [
        StructField(MixpanelColName.MP_TIMESTAMP.value, IntegerType(), False),
        StructField(MixpanelColName.MP_DISTINCT_ID.value, StringType(), False),
        StructField(MixpanelColName.MP_INSERT_ID.value, StringType(), False),
        StructField(MixpanelColName.CS_USER_ID.value, StringType(), True),
        StructField(MixpanelColName.CS_VERSION_ID.value, StringType(), True),
        StructField(MixpanelColName.MP_DT.value, TimestampType(), False),
    ]


def add_client_must_have_cols():
    return [
        StructField(MixpanelColName.CS_PLATFORM.value, StringType(), True),
        add_device(),
        StructField("screen_height", IntegerType(), True),
        StructField("screen_width", IntegerType(), True),
        StructField(MixpanelColName.CS_CLIENT.value, StringType(), True),
    ]


def add_client_web_default_cols():
    return [
        StructField("browser", StringType(), True),
        StructField("browser_version", StringType(), True),
        StructField("city", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("os", StringType(), True),
        StructField("region", StringType(), True),
        StructField("mp_country_code", StringType(), True),
        StructField("referring_domain", StringType(), True),
    ]


def add_other(col_name: str, data_type=StringType(), nullable=True):
    return StructField(col_name, data_type, nullable)


def add_room_id(nullable=True):
    return add_other(MixpanelColName.CS_ROOM_ID.value)


def add_lesson_id(nullable=True):
    return add_other(MixpanelColName.CS_LESSON_ID.value)


def add_device(nullable=True):
    return add_other(MixpanelColName.MP_DEVICE.value)


def add_role(nullable=True):
    return add_other(MixpanelColName.CS_ROLE.value)


def add_trigger_type(nullable=True):
    return add_other(MixpanelColName.CS_TRIGGER_TYPE.value)


def generate_schema_by_event(event: str):
    fields = add_must_have_cols()

    if event == MixpanelEvent.DISCONNECT.value:
        fields.append(add_room_id())
        fields.append(add_lesson_id())
        fields.append(add_role())

    elif event == MixpanelEvent.RECONNECT.value:
        fields.append(add_room_id())
        fields.append(add_lesson_id())
        fields.append(add_role())

    elif event == MixpanelEvent.LOGIN.value:
        fields += add_client_must_have_cols()
        fields.append(add_other(MixpanelColName.CS_LOGIN_TYPE.value))

    elif event == MixpanelEvent.LOGOUT.value:
        fields += add_client_must_have_cols()
        fields.append(add_room_id())

    elif event == MixpanelEvent.LESSON_START.value:
        fields += add_client_must_have_cols()
        fields.append(add_room_id())
        fields.append(add_lesson_id())

    elif event == MixpanelEvent.LESSON_END.value:
        fields += add_client_must_have_cols()
        fields.append(add_room_id())
        fields.append(add_lesson_id())

    elif event == MixpanelEvent.STUD_JOIN.value:
        fields += add_client_must_have_cols()
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_trigger_type())
        fields += add_client_web_default_cols()

    elif event == MixpanelEvent.STUD_LEAVE.value:
        fields += add_client_must_have_cols()
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_trigger_type())
        fields += add_client_web_default_cols()

    elif event == MixpanelEvent.PUSH_BTN.value:
        fields += add_client_must_have_cols()
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_other(MixpanelColName.CS_TASK_ID.value))
        fields.append(add_other(MixpanelColName.CS_PUSH_TYPE.value))

    elif event == MixpanelEvent.QUIZ_START.value:
        fields += add_client_must_have_cols()
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_other(MixpanelColName.CS_QUIZ_ID.value))
        fields.append(add_other(MixpanelColName.CS_QUIZ_TYPE.value))

    elif event == MixpanelEvent.QUIZ_END.value:
        fields += add_client_must_have_cols()
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_other(MixpanelColName.CS_QUIZ_ID.value))
        fields.append(add_other(MixpanelColName.CS_QUIZ_TYPE.value))

    return StructType(fields)
