from pyspark.sql.types import (
    StructType,
    TimestampType,
    StructField,
    StringType,
    IntegerType,
)
from utility.constants import MxpCol, MxpEvent


def add_must_have_cols():
    return [
        StructField(MxpCol.MP_TIMESTAMP.value, IntegerType(), False),
        StructField(MxpCol.MP_DISTINCT_ID.value, StringType(), False),
        StructField(MxpCol.MP_INSERT_ID.value, StringType(), False),
        StructField(MxpCol.CS_USER_ID.value, StringType(), True),
        StructField(MxpCol.CS_VERSION_ID.value, StringType(), True),
        StructField(MxpCol.MP_DT.value, TimestampType(), False),
    ]


def add_client_must_have_cols():
    return [
        StructField(MxpCol.CS_PLATFORM.value, StringType(), True),
        add_device(),
        StructField("screen_height", IntegerType(), True),
        StructField("screen_width", IntegerType(), True),
        StructField(MxpCol.CS_CLIENT.value, StringType(), True),
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


def add_other(col_name: str, data_type=None, nullable=True):
    if data_type:
        return StructField(col_name, data_type, nullable)
    else:
        return StructField(col_name, StringType(), nullable)


def add_room_id(nullable=True):
    return add_other(MxpCol.CS_ROOM_ID.value)


def add_lesson_id(nullable=True):
    return add_other(MxpCol.CS_LESSON_ID.value)


def add_device(nullable=True):
    return add_other(MxpCol.MP_DEVICE.value)


def add_role(nullable=True):
    return add_other(MxpCol.CS_ROLE.value)


def add_trigger_type(nullable=True):
    return add_other(MxpCol.CS_TRIGGER_TYPE.value)


def add_session_id(nullable=True):
    return add_other(MxpCol.CS_SESSION.value)


def generate_schema_by_event(event: str):
    fields = add_must_have_cols()

    if event == MxpEvent.DISCONNECT.value:
        fields.append(add_room_id())
        fields.append(add_lesson_id())
        fields.append(add_role())

    elif event == MxpEvent.RECONNECT.value:
        fields.append(add_room_id())
        fields.append(add_lesson_id())
        fields.append(add_role())

    elif event == MxpEvent.LOGIN.value:
        fields += add_client_must_have_cols()
        fields.append(add_session_id())
        fields.append(add_other(MxpCol.CS_LOGIN_TYPE.value))

    elif event == MxpEvent.LOGOUT.value:
        fields += add_client_must_have_cols()
        fields.append(add_session_id())
        fields.append(add_room_id())

    elif event == MxpEvent.LESSON_START.value:
        fields += add_client_must_have_cols()
        fields.append(add_session_id())
        fields.append(add_room_id())
        fields.append(add_lesson_id())

    elif event == MxpEvent.LESSON_END.value:
        fields += add_client_must_have_cols()
        fields.append(add_session_id())
        fields.append(add_room_id())
        fields.append(add_lesson_id())

    elif event == MxpEvent.STUD_JOIN.value:
        fields += add_client_must_have_cols()
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_trigger_type())
        fields += add_client_web_default_cols()

    elif event == MxpEvent.STUD_LEAVE.value:
        fields += add_client_must_have_cols()
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_trigger_type())
        fields += add_client_web_default_cols()

    elif event == MxpEvent.PUSH_BTN.value:
        fields += add_client_must_have_cols()
        fields.append(add_session_id())
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_other(MxpCol.CS_TASK_ID.value))
        fields.append(add_other(MxpCol.CS_PUSH_TYPE.value))

    elif event == MxpEvent.QUIZ_START.value:
        fields += add_client_must_have_cols()
        fields.append(add_session_id())
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_other(MxpCol.CS_QUIZ_ID.value))
        fields.append(add_other(MxpCol.CS_QUIZ_TYPE.value))

    elif event == MxpEvent.QUIZ_END.value:
        fields += add_client_must_have_cols()
        fields.append(add_session_id())
        fields.append(add_lesson_id())
        fields.append(add_room_id())
        fields.append(add_other(MxpCol.CS_QUIZ_ID.value))
        fields.append(add_other(MxpCol.CS_QUIZ_TYPE.value))

    elif event == MxpEvent.APP_ENV_INFO.value:
        # client
        fields.append(add_other(MxpCol.CS_PLATFORM.value))
        fields.append(add_other(MxpCol.SCREEN_HEIGHT.value, IntegerType(), True))
        fields.append(add_other(MxpCol.SCREEN_WIDTH.value, IntegerType(), True))
        fields.append(add_other(MxpCol.CS_CLIENT.value))
        # others
        fields.append(add_other("os"))
        fields.append(add_other("chassis_type"))
        fields.append(add_other("cpu_name"))
        fields.append(add_other("manufacturer"))
        fields.append(add_other("model"))
        fields.append(add_other(MxpCol.CS_COUNTRY_CODE.value))
        fields.append(add_other("screens"))
        fields.append(add_session_id())
        fields.append(add_other(MxpCol.VS_REGION.value))

    return StructType(fields)
