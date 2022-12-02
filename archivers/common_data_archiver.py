from psycopg2._psycopg import AsIs


class CommonDataArchiver:
    def __init__(self, conn, in_schemas, logger, task_type):
        self.conn = conn
        self.in_schemas = in_schemas
        self.logger = logger  # unused
        self.task_type = task_type
        self.config = in_schemas[task_type]

    def common_run(self, task_type: str):
        try:
            self.archive_tables()
            self.conn.conn.commit()
            status = True
        except Exception as e:
            self.logger.error(f"[НЕОБРАБОТАННАЯ ОШИБКА] При архивации {task_type} возникла неизвестная ошибка:\n"
                              f"Откат изменений.\n{e}!")
            self.conn.conn.rollback()
            status = False
        return status

    def archive_tables(self):
        raise NotImplementedError("Must override archive_tables")

    def copy_table(self, from_table, to_table,
                   from_schema=None, to_schema=None,
                   where_col=None, equals_to=None):
        """
        Copies the table in an append-like fashion

        :param from_table:
        :param to_table:
        :param from_schema:
        :param to_schema:
        :param where_col:
        :param equals_to:
        :return:
        """

        if from_schema is None: from_schema = self.in_schemas[self.task_type]["main_schema"]["schema_name"]
        if to_schema is None: to_schema = self.in_schemas[self.task_type]["archive_schema"]["schema_name"]
        from_columns = self._get_intersecting_columns(from_schema, from_table, to_schema, to_table)
        from_schema_table = self._sql_name(from_schema, from_table)
        to_schema_table = self._sql_name(to_schema, to_table)

        if equals_to is None:
            self.conn.copy_table(from_schema_table, to_schema_table, from_columns)
            print(f"Table '{from_schema_table}' copied to table '{to_schema_table}'")

        else:
            self.conn.copy_table_where(from_schema_table, to_schema_table,
                                       from_columns, where_col, equals_to)
            print(f"Table '{from_schema_table}' copied to table '{to_schema_table}''"
                  f" WHERE '{where_col}' == {equals_to}")

    def copy_tables(self, from_tables, to_tables, where_cols, equal_to_values):
        """
        Аналог функции copy_table, однако используемый для нескольких таблиц. from_tables, to_tables,
        where_cols, equal_to_values должны быть переданы как list обязательно.

        :param from_tables:
        :param to_tables:
        :param where_cols: list. Колонки, по которым проходит фильтрация,
        :param equal_to_values: list. Значения на которые проверяются экспортируемые ряды.
        :return:
        """
        self._check_for_length(from_tables, to_tables)
        equal_to_values, where_cols = self._normalise_filter_values(equal_to_values, where_cols, len(from_tables))

        for from_table, to_table, where_col, equals_to_ \
                in zip(from_tables, to_tables, where_cols, equal_to_values):
            self.copy_table(from_table, to_table,
                            where_col=where_col, equals_to=equals_to_)

    def delete_table(self, table, where_col, equal_to, schema=None):
        if schema is None: schema = self.in_schemas[self.task_type]["main_schema"]["schema_name"]
        schema_table_name = self._sql_name(schema, table)

        self.conn.delete_table(schema_table_name, where_col, equal_to)
        print(f"Rows of Table '{schema_table_name} WHERE '{where_col}' == {equal_to} are DELETED")

    def delete_tables(self, tables, where_cols, equal_to_values):
        equal_to_values, where_cols = self._normalise_filter_values(equal_to_values, where_cols, len(tables))

        for table, where_col, equal_to in zip(tables, where_cols, equal_to_values):
            self.delete_table(table, where_col, equal_to)

    def _get_intersecting_columns(self, from_schema, from_table, to_schema, to_table):
        from_columns = self.conn.get_column_names(self._sql_name(from_schema, from_table))
        to_columns = self.conn.get_column_names(self._sql_name(to_schema, to_table))
        intersect_cols = list(set(from_columns) & set(to_columns))
        formatted_cols = ", ".join(intersect_cols)
        return formatted_cols

    def _normalise_filter_values(self, equal_to_values, where_cols, elements_num):
        if len(equal_to_values) == len(where_cols) == 1:
            self._check_for_filter_types(equal_to_values, where_cols)
            equal_to_values = equal_to_values * elements_num
            where_cols = where_cols * elements_num
        assert len(equal_to_values) == elements_num
        assert len(where_cols) == elements_num

        return equal_to_values, where_cols

    def _check_for_filter_types(self, equal_to_values, where_cols):
        if type(equal_to_values) != list != type(where_cols):
            raise TypeError(
                f'{self.copy_tables.__name__}: параметры equal_to_values и where_cols обязаны быть list!')

    def _check_for_length(self, from_tables, to_tables):
        if len(from_tables) != len(to_tables):
            raise IndexError(
                f'{self.copy_tables.__name__}: trying to pass different number of from_tables and to_tables')

    def get_json_table_names(self, schema, without=None):
        tables = self.config[schema]["tables"]
        if without is not None:
            without_name = tables[without]["table_name"]
        else:
            without_name = None

        names = [v["table_name"] for k, v in tables.items() if v["table_name"] != without_name]
        return names

    @staticmethod
    def _sql_name(schema_name, table_name):
        schema_and_table = f"{schema_name}.{table_name}"
        return AsIs(schema_and_table)
