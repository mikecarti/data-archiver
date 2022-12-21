import json
import os
import warnings
import logging
import logging.config

from archivers.common_data_archiver import DataArchiver


class DataArchiverInterface:

    def __init__(self, conn, log_stream):
        self.conn = conn
        self.in_schemas = self.parse_json('json_files/table_data_schemas.json')
        self.workdir = os.path.dirname(os.path.realpath(__file__))
        self.data_folder = r"test_common_data/upload_files/"
        self.data_folder_report = r"test_common_data/download_files/"
        if self.workdir == r"/var/lib":
            self.data_folder = r"/var/lib/data_folder"
            self.data_folder_report = r"/var/lib/data_destination"
        self.stream = log_stream
        self.logger = self.config_task_logger(logger_name="task_logger", logger_filename="task_log.log",
                                              stream=self.stream)
        # warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
        warnings.simplefilter(action='ignore', category=UserWarning)

    @staticmethod
    def parse_json(path):
        with open(path, 'r', encoding='utf-8') as f:
            json_dict = json.load(f)
        return json_dict

    def run(self, d):
        self.comment = ""

        task_type = d["file_type"]

        if self.task_is_in_json(task_type):
            archiver = DataArchiver(conn=self.conn, in_schemas=self.in_schemas, logger=self.logger,
                                    task_type=task_type)
            status = archiver.run(d)
        else:
            self.logger.error(f"Unregistered task_type: {task_type}")
            status, comment = False, "[Upload Error] Неизвестный тип файла!"

        comment = self.stream.getvalue()
        self.stream.seek(0)
        self.stream.truncate(0)
        return status, comment

    def config_task_logger(self, logger_name, logger_filename, stream):
        DEFAULT_LOGGING = {
            'version': 1,
            'formatters': {
                'standard': {
                    'format': '%(asctime)s %(levelname)s: %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S'},
            },
            'handlers': {
                'console': {'class': 'logging.StreamHandler',
                            'formatter': "standard",
                            'level': 'DEBUG',
                            'stream': stream},
                'file': {'class': 'logging.FileHandler',
                         'formatter': "standard",
                         'level': 'DEBUG',
                         'filename': logger_filename,
                         'mode': 'w',
                         'encoding': "utf-8"}
            },
            'loggers': {
                logger_name: {'level': 'DEBUG',
                              'handlers': ['console', 'file'],
                              'propagate': False},
            }
        }
        logging.config.dictConfig(DEFAULT_LOGGING)
        log = logging.getLogger(logger_name)
        return log

    def task_is_in_json(self, task_type):
        possible_tasks = list(self.in_schemas.keys())
        return task_type in possible_tasks


if __name__ == "__main__":
    data_folder = r"C:\PythonProjects\RabbitMQ_test\ebitda-data-manager\test_common_data"
    file_loc = r"upload_files\1_year\scenario\production_graph.xlsx"
    _abs_path = os.path.join(data_folder, file_loc)

    du = DataArchiverInterface()
    du.parse_excel(_abs_path)
