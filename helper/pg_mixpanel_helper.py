from dao.pg_dao import PGDAODW

pg_dao = PGDAODW()
schema = "mixpanel"


def query_max_epoch_time(table: str) -> int:
    sql = f"select max(mp_ts) from {schema}.{table}"
    cursor = pg_dao.find_by_sql(sql)
    max_time, *_ = cursor.fetchone()
    return max_time


def delete_by_epoch_time(table: str, start_epoch: int) -> int:
    sql = f"delete from {schema}.{table} where mp_ts >= %s"
    return pg_dao.execute_by_sql(sql, (start_epoch,))
