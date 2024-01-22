import psycopg2


class PGDAO:
    def __init__(self, conn_config: dict):
        self.conn = psycopg2.connect(**conn_config)

    def find_by_sql(self, sql_text: str, para=None):
        cursor = self.conn.cursor()
        if para:
            cursor.execute(sql_text, para)
        else:
            cursor.execute(sql_text)
        return cursor

    def execute_by_sql(self, sql_text: str, para=None):
        with self.conn.cursor() as cursor:
            if para:
                cursor.execute(sql_text, para)
            else:
                cursor.execute(sql_text)
            rowcount = cursor.rowcount
        self.conn.commit()
        return rowcount


class PGDAODW(PGDAO):
    def __init__(self):
        from config import PG_WH_PROD_CONFIG

        PG_WH_PROD_CONFIG["dbname"] = "warehouse"
        super().__init__(PG_WH_PROD_CONFIG)
