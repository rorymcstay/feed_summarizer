from feed.logger import getLogger
import logging
import os
from src.main.parser import ResultParser
from feed.actionchains import KafkaActionSubscription
from feed.settings import nanny_params
from logging.config import dictConfig


class CaptureActionRunner(KafkaActionSubscription):
    def __init__(self):
        queue = f'{os.getenv("KAFKA_TOPIC_PREFIX", "u")}-summarizer-route'
        logging.info(f'subscribing to {queue}')
        KafkaActionSubscription.__init__(self, topic=queue, implementation=ResultParser)

if __name__ == '__main__':
    dictConfig({
        'version': 1,
        'formatters': {'default': {
            'format': '[%(asctime)s]%(thread)d: %(module)s - %(levelname)s - %(message)s',
        }},
        'handlers': {'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default'
        }},
        'root': {
            'level': 'INFO',
            'handlers': ['wsgi']
        }
    })

    logging.getLogger("urllib3").setLevel("INFO")

    logging.info("####### Environment #######")
    logging.info(f'nanny: {nanny_params}')
    logging.info("\n".join([f'{key}={os.environ[key]}' for key in os.environ]))

    logging.info('Beginning capture-crawler initialisation')
    logging.info("\n".join([f'{key}={os.environ[key]}' for key in os.environ]))
    runner = CaptureActionRunner()
    logging.info(f'initialised {runner}')
    runner.main()
