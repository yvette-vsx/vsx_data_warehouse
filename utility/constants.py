from enum import Enum


class EnviroType(Enum):
    PROD = 0
    STAGE = 1
    DEV = 2


class MixpanelColName(Enum):
    MP_TIMESTAMP = "mp_ts"
    MP_DISTINCT_ID = "distinct_id"
    MP_INSERT_ID = "insert_id"
    CS_USER_ID = "cs_user_id"
    CS_VERSION_ID = "cs_version"
    CS_CLIENT = "cs_client"
    CS_ROLE = "cs_role"


class MixpanelEvent(Enum):
    DISCONNECT = "disconnect"
    STUD_LEAVE = "studentleave"
    LESSON_END = "lessonend"
    LESSON_START = "lessonstart"
    LOGIN = "login"
    LOGOUT = "logout"
    RECONNECT = "reconnect"
    STUD_JOIN = "studentjoin"
