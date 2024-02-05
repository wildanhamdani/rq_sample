from rq import Worker, Queue, Connection
from redis import Redis
import logging
import sys

from worker import queue_message_text
from datetime import datetime

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)

if __name__ == "__main__":

    data_redis_host = "127.0.0.1"
    data_redis_port = 6379
    data_redis_db = 0
    data_redis_password = False
    queue_name = "queue-process"

    if data_redis_password:
        redis_conn = Redis(
            host=data_redis_host,
            port=data_redis_port,
            db=data_redis_db,
            password=data_redis_password,
        )
    else:
        redis_conn = Redis(
            host=data_redis_host, port=data_redis_port, db=data_redis_db
        )

    for i in range(10):
        with Connection(redis_conn):
            message = f"Hello from main {i}"
            message_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            q = Queue(connection=redis_conn, name=queue_name)
            q.enqueue(queue_message_text, message, message_time)
            logging.info(f"Enqueue message: {message_time} : {message}")
