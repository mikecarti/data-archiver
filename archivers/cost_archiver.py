from .common_data_archiver import CommonDataArchiver


class CostArchiver(CommonDataArchiver):

    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)
        self.meta_dataset_id = None

    def run(self, d: dict):
        self.logger.info("Cost Archiver running")
        self.meta_dataset_id = d["metaload_dataset_id"]

        return self.common_run(task_type=d['file_type'])


