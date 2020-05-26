from feed.logger import getLogger
import logging
import os
from src.main.parser import ResultParser
from feed.actionchains import KafkaActionSubscription
from feed.settings import nanny_params, logger_settings_dict
from logging.config import dictConfig


class CaptureActionRunner(KafkaActionSubscription):
    def __init__(self):
        queue = f'summarizer-route'
        logging.info(f'subscribing to {queue}')
        KafkaActionSubscription.__init__(self, topic=queue, implementation=ResultParser)

if __name__ == '__main__':
    dictConfig(logger_settings_dict)

    logging.getLogger("urllib3").setLevel("INFO")

    logging.info("####### Environment #######")
    logging.info(f'nanny: {nanny_params}')
    logging.info("\n".join([f'{key}={os.environ[key]}' for key in os.environ]))

    logging.info('Beginning capture-crawler initialisation')
    logging.info("\n".join([f'{key}={os.environ[key]}' for key in os.environ]))
    runner = CaptureActionRunner()
    logging.info(f'initialised {runner}')
    runner.main()
