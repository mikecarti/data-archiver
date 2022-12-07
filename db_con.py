import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import execute_values
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter

import os


class DBCon:
    def __init__(self, _dbname, _dbuser, _dbpass, _host, _port):
        self.conn = psycopg2.connect(
            dbname=_dbname,
            user=_dbuser,
            password=_dbpass,
            host=_host,
            port=_port,
        )
        self.conn.autocommit = False
        register_adapter(dict, Json)
        return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def set_task_state(self, status_code, status_text, error_description, task_id, updated_at):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE task
                SET status_code=%s, status_text=%s, error_description=%s, updated_at=%s
                WHERE task_id = %s
                """,
                [status_code, status_text, error_description, updated_at, task_id],
            )

    def copy_table(self, from_schema_table: str, to_schema_table: str, columns: str):
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO %(to_schema_table)s ({columns})
                SELECT {columns} 
                FROM %(from_schema_table)s
                RETURNING {columns}
                """,
                {'to_schema_table': to_schema_table, 'from_schema_table': from_schema_table}
            )
            some_data = cur.fetchall()
            return some_data

    def copy_table_where(self, from_schema_table, to_schema_table, columns, where_col, equals_to):
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO %(to_schema_table)s ({columns})
                SELECT {columns} 
                FROM %(from_schema_table)s
                WHERE %(where_col)s = %(equals_to)s
                RETURNING %(where_col)s
                """,
                {
                    'to_schema_table': to_schema_table, 'from_schema_table': from_schema_table,
                    'where_col': AsIs(where_col), 'equals_to': AsIs(equals_to)
                }
            )
            where_col_data = cur.fetchall()
            return where_col_data

    def delete_table_where(self, schema_table, where_col, equal_to):
        with self.conn.cursor() as cur:
            cur.execute(
                f"""    
                DELETE FROM %(schema_and_table)s
                WHERE %(column_name)s = %(value)s
                RETURNING %(column_name)s
                """,
                {'schema_and_table': schema_table, 'column_name': AsIs(where_col), 'value': equal_to}
            )
            where_col_data = cur.fetchall()
            return where_col_data

    def get_column_names(self, schema_table_name):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM %(schema_table_name)s LIMIT 0
                """,
                {'schema_table_name': schema_table_name}
            )
            col_names = [desc[0] for desc in cur.description]
            return col_names


if __name__ == "__main__":
    rabbit_broker = "localhost"
    db_name = os.environ['DB_NAME'] if 'DB_NAME' in os.environ else "ebitda"
    db_user = os.environ['DB_USER'] if 'DB_USER' in os.environ else "postgres"
    db_pass = os.environ['DB_PASS'] if 'DB_PASS' in os.environ else "Chd56@&rT"
    db_host = os.environ['DB_HOST'] if 'DB_HOST' in os.environ else "localhost"
    db_port = int(os.environ['DB_PORT']) if 'DB_PORT' in os.environ else 5482

    with DBCon(db_name, db_user, db_pass, db_host, db_port) as conn:
        print(conn)
