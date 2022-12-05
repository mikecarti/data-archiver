from .common_data_archiver import CommonDataArchiver


class MacroeconomicsArchiver(CommonDataArchiver):
    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)
        # self.meta_dataset_id = None

    def run(self, d: dict):
        print("Macroeconomics Archiver running")

        return self.common_run(task_type=d['file_type'])

    def archive_tables(self):
        self.copy_macroeconomics_tables()
        self.delete_macroeconomics_tables()

    def copy_macroeconomics_tables(self):
        pass

    def delete_macroeconomics_tables(self):
        pass
