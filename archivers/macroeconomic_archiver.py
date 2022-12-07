from .common_data_archiver import CommonDataArchiver


class MacroeconomicsArchiver(CommonDataArchiver):
    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)
        self.meta_dataset_id = None

    def run(self, d: dict):
        self.logger.info("Macroeconomics Archiver running")
        self.meta_dataset_id = d["metaload_dataset_id"]

        return self.common_run(task_type=d['file_type'])

    def archive_tables(self):
        self.copy_metadata_entry()
        self.prepare_copying_tables(from_schema="main_schema", to_schema="archive_schema")
        self.prepare_deleting_tables(schema="main_schema")
        self.delete_metadata_entry()

