from psycopg2._psycopg import AsIs
import traceback
import warnings


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
                              f"{e}!\n\n Откат изменений")
            traceback.print_exc()
            traceback.format_exc()
            self.conn.conn.rollback()
            status = False
        return status

    def prepare_copying_tables(self):
        from_tables = self._get_db_table_names("main_schema", without="upload_files")
        to_tables = self._get_db_table_names("main_schema", without="upload_files")

        from_tables_json_names = self._get_json_table_names(schema="main_schema", without="upload_files")

        metaload_id_cols = self._get_required_columns_names_for_bd(json_column_name="metaload_dataset_id",
                                                                   schema="main_schema",
                                                                   tables_json_names=from_tables_json_names)

        self.copy_tables(from_tables, to_tables, where_cols=metaload_id_cols,
                         equal_to_values=[self.meta_dataset_id])

    def prepare_deleting_tables(self):
        tables_db_names = self._get_db_table_names("main_schema", without="upload_files")
        tables_json_names = self._get_json_table_names("main_schema", without="upload_files")

        metaload_id_cols = self._get_required_columns_names_for_bd(json_column_name="metaload_dataset_id",
                                                                   schema="main_schema",
                                                                   tables_json_names=tables_json_names)

        self.delete_tables(tables_db_names, where_cols=metaload_id_cols, equal_to_values=[self.meta_dataset_id])



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
            data = self.conn.copy_table(from_schema_table, to_schema_table, from_columns)
        else:
            data = self.conn.copy_table_where(from_schema_table, to_schema_table,
                                              from_columns, where_col, equals_to)
            specification = f"ГДЕ '{where_col}' == {equals_to}"

        self._report_results(data, from_schema_table, specification, to_schema_table)

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

        data = self.conn.delete_table_where(schema_table_name, where_col, equal_to)
        self.logger.info(f"Ряды таблицы '{schema_table_name} ГДЕ '{where_col}' == {equal_to} УДАЛЕНЫ")
        if self._query_result_empty(data):
            self._report_results(data, schema_table_name)
        self._report_empty_result(data, schema_table_name)

    def delete_tables(self, tables: list[str], where_cols: list[str], equal_to_values: list[object]):
        equal_to_values, where_cols = self._normalise_filter_values(equal_to_values, where_cols, len(tables))

        for table, where_col, equal_to in zip(tables, where_cols, equal_to_values):
            self.delete_table(table, where_col, equal_to)

    def copy_metadata_entry(self):
        """
        copies metadata_table entry from public_schema to archive_schema
        :return:
        """
        pub_upload = self.config["main_schema"]["tables"]["upload_files"]
        arch_upload = self.config["archive_schema"]["tables"]["upload_files"]

        from_table = pub_upload["table_name"]
        to_table = arch_upload["table_name"]
        meta_dataset_col = pub_upload["req_cols"]["metaload_dataset_id"]

        self.copy_table(from_table, to_table,
                        where_col=meta_dataset_col, equals_to=self.meta_dataset_id)

    def delete_metadata_entry(self):
        """
        deletes metadata_table entry from public_schema
        :return:
        """
        upload_files_table = self.config["main_schema"]["tables"]["upload_files"]
        table = upload_files_table["table_name"]
        meta_dataset_col = upload_files_table["req_cols"]["metaload_dataset_id"]
        self.delete_table(table=table, where_col=meta_dataset_col, equal_to=self.meta_dataset_id)

    def _report_results(self, data, from_schema_table, to_schema_table, where_col, equal_to, specification, mode=None):
        match mode:
            case "copy":
                if self._query_result_empty(data):
                    self.logger.warning(f"Из таблицы '{from_schema_table}' не было найдено ни одного ряда по данному фильтру!")
                else:
                    self.logger.info(f"Таблица '{from_schema_table}' скопирована в таблицу '{to_schema_table}' {specification}")
            case "delete":
                if self._query_result_empty(data):
                    self.logger.warning(f"Из таблицы '{from_schema_table}' не было найдено ни одного ряда по данному фильтру!")
                else:
                    self.logger.info(f"Ряды таблицы '{from_schema_table} ГДЕ '{where_col}' == {equal_to} УДАЛЕНЫ")
            case _:
                raise NotImplementedError()

    def _get_intersecting_columns(self, from_schema, from_table, to_schema, to_table):
        from_columns = self.conn.get_column_names(self._sql_name(from_schema, from_table))
        to_columns = self.conn.get_column_names(self._sql_name(to_schema, to_table))
        intersect_cols = list(set(from_columns) & set(to_columns))
        formatted_cols = ", ".join(intersect_cols)
        return formatted_cols

    def _get_required_column_name_for_bd(self, schema, table_name, json_column_name):
        """
        Возвращает имя колонки в бд, ориентируясь на данные в JSON
        :param schema:
        :param table_name: json имя таблицы (не то, которое указано в БД)
        :param json_column_name:
        :return: metaload_id_col
        """
        return self.in_schemas[self.task_type][schema]["tables"][table_name]["req_cols"][json_column_name]

    def _get_required_columns_names_for_bd(self, schema: str,
                                           tables_json_names: list[str],
                                           json_column_name: str) -> list[str]:
        """
        Возвращает имя колонки из БД, для каждой таблицы *tables_json_names*
        :param schema:
        :param tables_json_names:
        :param json_column_name:
        :return:
        """
        metaload_id_cols = []
        for table_name in tables_json_names:
            metaload_id_cols.append(self._get_required_column_name_for_bd(schema,
                                                                          table_name=table_name,
                                                                          json_column_name=json_column_name))
        return metaload_id_cols

    def _normalise_filter_values(self, equal_to_values, columns_names, elements_num):
        if len(equal_to_values) == len(columns_names) == 1:
            self._check_for_filter_types(equal_to_values, columns_names)
            equal_to_values = equal_to_values * elements_num
            columns_names = columns_names * elements_num
        elif len(columns_names) > 1 and len(equal_to_values) == 1:
            equal_to_values = equal_to_values * len(columns_names)
        assert len(equal_to_values) == elements_num, f"Ошибка, len(equal_to_values): {len(equal_to_values)}, elements_num: {elements_num}"
        assert len(columns_names) == elements_num, f"Ошибка, len(columns_names): {len(columns_names)}, elements_num: {elements_num}"

        return equal_to_values, columns_names

    def _check_for_filter_types(self, equal_to_values, where_cols):
        if type(equal_to_values) != list != type(where_cols):
            raise TypeError(
                f'{self.copy_tables.__name__}: параметры equal_to_values и where_cols обязаны быть list!')

    def _check_for_length(self, from_tables, to_tables):
        if len(from_tables) != len(to_tables):
            raise IndexError(
                f'{self.copy_tables.__name__}: trying to pass different number of from_tables and to_tables')

    def _get_db_table_names(self, schema, without=None):
        """
        Возвращает имена колонок в таблице, так как они предположительно отображены в базе данных. Информация
        берется из JSON типа read-only.
        :param schema:
        :param without: json имя таблицы
        :return:
        """
        tables = self.config[schema]["tables"]
        if without is not None:
            without_name = tables[without]["table_name"]
        else:
            without_name = None

        names = [v["table_name"] for k, v in tables.items() if v["table_name"] != without_name]
        return names

    def _get_json_table_names(self, schema, without=None):
        """
        Возвращает JSON имена колонок в таблице. Эти имена не обязаны соответствовать именам колонок в бд.
        Информация берется из JSON файла типа read-only.
        :param schema:
        :param without: json имя таблицы
        :return:
        """
        tables = self.config[schema]["tables"]
        names = [k for k, v in tables.items() if k != without]
        return names

    @staticmethod
    def _sql_name(schema_name, table_name):
        schema_and_table = f"{schema_name}.{table_name}"
        return AsIs(schema_and_table)

    def _report_empty_result(self, data, schema_table_name):
        if self._query_result_empty(data):
            self.logger.warning(f"Из таблицы '{schema_table_name}' не было найдено ни одного ряда по данному фильтру!")
            return True
        return False
    def _query_result_empty(self, data):
        empty_list = len(data) == 0
        none_type = not data

        return any([empty_list, none_type])
