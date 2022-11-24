import pika
import json
import os
import time
import functools
from psycopg2.errors import OperationalError
from pika.exceptions import AMQPConnectionError
from db_con import DBCon
import pandas as pd
from io import StringIO

from interface_data_archiver import DataArchiver

TASK_TOPIC = "archive_queue"
SOCKET_TOPIC = "Setting"


class DataManager:

    def __init__(self, db_name, db_user, db_pass, db_host, db_port, rabbit_broker, rabbit_port):
        connection_params = pika.ConnectionParameters(host=rabbit_broker, port=rabbit_port)
        self.conn = pika.BlockingConnection(connection_params)
        self.ch = self.conn.channel()
        self.ch.queue_declare(TASK_TOPIC)
        # Очистить очередь
        self.ch.queue_purge(queue=TASK_TOPIC)
        # Очередь для сокета
        self.ch_socket = self.conn.channel()
        # self.ch_socket.queue_declare(queue=SOCKET_TOPIC)

        # Связь с БД
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_host = db_host
        self.db_port = db_port

    def run(self):
        try:
            with DBCon(self.db_name, self.db_user, self.db_pass, self.db_host, self.db_port) as conn:
                print("----------------Connection to db established----------------")
                # print("NOW WE CAN CHANGE DOCKER IN REAL TIME ONCE AGAIN")
                stream = StringIO()
                data_archiver = DataArchiver(conn, stream)
                callback = functools.partial(self.on_message, data_archiver=data_archiver)
                self.ch.basic_consume(queue=TASK_TOPIC, on_message_callback=callback)
                self.ch.start_consuming()
        except KeyboardInterrupt:
            self.ch.stop_consuming()
        self.conn.close()
        stream.close()

    def on_message(self, ch, method, properties, body, data_archiver=None):
        # data_uploader.conn.get_table_data_types("upload_files")

        print(" [x] Received %r" % json.loads(body))
        data_from_body = json.loads(body)
        status, comment = data_archiver.run(data_from_body)

        print(f" [x] Received status: {status}, comment:\n{comment}")

        status_code = 2 if status else 3
        status_text = "Файл успешно загружен." if status else "Файл загружен с ошибкой!"
        cur_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        data_archiver.conn.set_task_state(status_code, status_text, comment, data_from_body["task_id"], cur_time)

        socket_data = self.get_data_for_socket(
            status_code, status_text, data_from_body["task_id"], cur_time, data_from_body["file_type"],
            data_from_body["file_id"], data_from_body["metaload_user_id"])
        self.ch_socket.basic_publish(
            exchange="amq.direct",
            routing_key=SOCKET_TOPIC,
            body=json.dumps(socket_data)
        )
        time.sleep(1)
        print(" [x] Done")
        print()
        self.ch.basic_ack(delivery_tag=method.delivery_tag)

    def get_data_for_socket(self, status_code, status_text, task_id, cur_time, file_type, file_id, user_id):
        type_notify = "success" if status_code == 2 else "error"
        text = "успешно." if status_code == 2 else "с ошибкой!"
        data = {
            "entity": 'Setting',
            "entityId": task_id,
            "event": 'upload_file',
            'datetime': cur_time,
            'extData': {
                'task_id': task_id,
                'task_type': 'upload_file',
                'type_notify': type_notify,
                'task_status_code': status_text,
                'user_id': user_id,
                'file_id': file_id,
                'file_type': file_type,
                'title': status_text,
                'text': f'Задача с идентификатором {task_id} выполнена {text} Для просмотра задачи пройдите по <a href="/task/{task_id}/edit">Ссылке</a>'
            }
        }
        return data


if __name__ == "__main__":
    rabbit_broker = os.environ['RABBIT_HOST'] if 'RABBIT_HOST' in os.environ else "localhost"
    rabbit_port = int(os.environ['RABBIT_PORT']) if 'RABBIT_PORT' in os.environ else 15555
    db_name = os.environ['DB_NAME'] if 'DB_NAME' in os.environ else "ebitda"
    db_user = os.environ['DB_USER'] if 'DB_USER' in os.environ else "postgres"
    db_pass = os.environ['DB_PASS'] if 'DB_PASS' in os.environ else "guest"
    db_host = os.environ['DB_HOST'] if 'DB_HOST' in os.environ else "localhost"
    db_port = int(os.environ['DB_PORT']) if 'DB_PORT' in os.environ else 5482  # для теста на локалке

    while True:
        try:
            print("Creating DataManager listener instance")
            cnsr = DataManager(db_name, db_user, db_pass, db_host, db_port, rabbit_broker, rabbit_port)
            cnsr.run()
        except (AMQPConnectionError) as e:
            print("Waiting for rabbit")
            time.sleep(5)
            pass
        except OperationalError:
            print("Wait for database")
            time.sleep(5)
            pass
        time.sleep(5)

