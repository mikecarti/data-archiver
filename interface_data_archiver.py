import json
import os
import warnings
import logging
import logging.config

from archivers.business_org_archiver import BusinessOrgArchiver
from archivers.production_graph_archiver import ProductGraphArchiver
from archivers.macroeconomic_archiver import MacroeconomicsArchiver


class DataArchiver:

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
        status, comment = False, "[Upload Error] Неизвестный тип файла!"

        task_type = self.get_task_type(d)
        match task_type:
            case "archive_business_orgs_spr":
                orgs_arch = BusinessOrgArchiver(self.conn, self.in_schemas, self.logger, task_type=task_type)
                status = orgs_arch.run(d)
            case "archive_production_graph":
                prod_arch = ProductGraphArchiver(self.conn, self.in_schemas, self.logger, task_type=task_type)
                status = prod_arch.run(d)
            case "archive_macroeconomic":
                macro_econ_arch = MacroeconomicsArchiver(self.conn, self.in_schemas, self.logger, task_type=task_type)
                status = macro_econ_arch.run(d)
            case _:
                print(f"Unregistered task_type: {task_type}")

        return status
        # comment = self.stream.getvalue()
        # self.stream.seek(0)
        # self.stream.truncate(0)
        # return status, comment

    def get_task_type(self, d):
        task_type = f"{d['type']}_{d['file_type']}"
        return task_type

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



if __name__ == "__main__":
    data_folder = r"C:\PythonProjects\RabbitMQ_test\ebitda-data-manager\test_common_data"
    file_loc = r"upload_files\1_year\scenario\production_graph.xlsx"
    _abs_path = os.path.join(data_folder, file_loc)

    du = DataArchiver()
    du.parse_excel(_abs_path)