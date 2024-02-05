from redis import Redis
from rq import Queue, Worker, Connection

import logging
import sys
import json

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)


def queue_message_text(message, message_time):
    logging.info(f"Process from Queue: {message_time} : {message}")


if __name__ == "__main__":

    data_redis_host = "127.0.0.1"
    data_redis_port = 6379
    data_redis_db = 0
    data_redis_password = False
    queue_name = "queue-process"

    logging.info("START PROCESS WORKER")
    logging.info(f"   redis queue host: {data_redis_host}")
    logging.info(f"   redis queue port: {data_redis_port}")
    logging.info(f"   redis queue db: {data_redis_db}")

    with Connection():
        if data_redis_password:
            redis = Redis(
                host=data_redis_host,
                port=data_redis_port,
                db=data_redis_db,
                password=data_redis_password,
            )
        else:
            redis = Redis(host=data_redis_host,
                          port=data_redis_port, db=data_redis_db)
        q = Queue(connection=redis, name=queue_name)
        Worker([q], connection=redis).work()
