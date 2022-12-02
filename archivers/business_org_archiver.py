from .common_data_archiver import CommonDataArchiver


class BusinessOrgArchiver(CommonDataArchiver):

    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)

    def run(self, d):
        print("Business Org Archiver running")

        return self.common_run(d['file_type'])

    def archive_tables(self):
        """
        Архивирует одну таблицу
        :return:
        """
        from_table = list(self.config["main_schema"]["tables"].keys())[0]
        to_table = list(self.config["archive_schema"]["tables"].keys())[0]
        self.copy_table(from_table, to_table)
