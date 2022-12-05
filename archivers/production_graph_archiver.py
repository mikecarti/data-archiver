from .common_data_archiver import CommonDataArchiver


class ProductGraphArchiver(CommonDataArchiver):

    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)
        self.meta_dataset_id = None

    def run(self, d: dict):
        print("Product Graph Archiver running")
        self.meta_dataset_id = d["metaload_dataset_id"]

        return self.common_run(task_type=d['file_type'])

    def archive_tables(self):
        self.copy_metadata_entry()
        self.copy_production_graph_tables()
        self.delete_production_graph_tables()
        self.delete_metadata_entry()

    def copy_production_graph_tables(self):
        from_tables = self._get_json_table_names("main_schema", without="upload_files")
        to_tables = self._get_json_table_names("main_schema", without="upload_files")

        from_tables_json_names = self._get_json_table_names(schema="main_schema", without="upload_files")

        metaload_id_cols = self._get_required_columns_names_for_bd(json_column_name="metaload_dataset_id",
                                                                   schema="main_schema",
                                                                   tables_json_names=from_tables_json_names)

        self.copy_tables(from_tables, to_tables, where_cols=metaload_id_cols,
                         equal_to_values=[self.meta_dataset_id])

    def delete_production_graph_tables(self):
        tables_db_names = self._get_db_table_names("main_schema", without="upload_files")
        tables_json_names = self._get_json_table_names("main_schema", without="upload_files")

        metaload_id_cols = self._get_required_columns_names_for_bd(json_column_name="metaload_dataset_id",
                                                                   schema="main_schema",
                                                                   tables_json_names=tables_json_names)

        self.delete_tables(tables_db_names, where_cols=metaload_id_cols, equal_to_values=[self.meta_dataset_id])
