from enum import Enum


class EnviroType(Enum):
    PROD = 0
    STAGE = 1
    DEV = 2


class MixpanelColName(Enum):
    """
    MP_: Default properties in mixpanel,
    CS_: Properties in Class Swift,
    DW_: Columns were added in data warehouse
    """

    MP_TIMESTAMP = "mp_ts"
    MP_DISTINCT_ID = "distinct_id"
    MP_INSERT_ID = "insert_id"
    CS_USER_ID = "cs_user_id"
    CS_VERSION_ID = "cs_version"
    CS_CLIENT = "cs_client"
    CS_ROLE = "cs_role"
    CS_ROOM_ID = "room_id"
    CS_CLASS_ID = "class_id"
    CS_LESSON_ID = "lesson_id"
    MP_DEVICE = "device"


class DWCommonColName(Enum):
    DW_CREATE_DATE = "create_date"
    DW_LAST_UPD_DATE = "last_upd_date"


class MixpanelEvent(Enum):
    DISCONNECT = "disconnect"
    STUD_LEAVE = "studentleave"
    LESSON_END = "lessonend"
    LESSON_START = "lessonstart"
    LOGIN = "login"
    LOGOUT = "logout"
    RECONNECT = "reconnect"
    STUD_JOIN = "studentjoin"
