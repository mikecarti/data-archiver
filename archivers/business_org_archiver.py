from .common_data_archiver import CommonDataArchiver


class BusinessOrgArchiver(CommonDataArchiver):

    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)

    def run(self, d):
        print("Business Org Archiver running")

        try:
            self.archive_tables()
            self.conn.conn.commit()
            status = True
        except Exception as e:
            self.logger.error(f"[НЕОБРАБОТАННАЯ ОШИБКА] При загрузке {d['file_type']} возникла неизвестная ошибка!\n "
                              f"Откат изменений.\n '{e}'")
            self.conn.conn.rollback()
            status = False
        return status

    def archive_table(self):
        from_table = list(self.config["main_schema"]["tables"].keys())[0]
        to_table = list(self.config["archive_schema"]["tables"].keys())[0]
        self.copy_table(from_table, to_table)
