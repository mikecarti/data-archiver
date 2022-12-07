from .common_data_archiver import CommonDataArchiver


class ProductionPlansArchiver(CommonDataArchiver):
    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)
        self.meta_dataset_id = None

    def run(self, d: dict):
        self.logger.info("Production Plans Archiver running")
        self.meta_dataset_id = d["metaload_dataset_id"]

        return self.common_run(task_type=d['file_type'])

    def archive_tables(self):
        self.copy_metadata_entry()
        self.prepare_copying_tables()
        self.prepare_deleting_tables()
        self.delete_metadata_entry()

