from feed.logger import getLogger
import logging
import os
import argparse
from src.main.parser import ResultParser
from feed.actionchains import KafkaActionSubscription

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)
logging.getLogger('kafka').setLevel(logging.WARNING)

sh = logging.StreamHandler()

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -%(message)s |%(filename)s:%(lineno)d')
sh.setFormatter(formatter)
logger.addHandler(sh)


argparser = argparse.ArgumentParser("Transform data from kafak feed to flatten structure")

argparser.add_argument('--produceObjects', action='store_true', default=False)

argparser.add_argument('--headlessMode', action='store_true', default=False)

class CaptureActionRunner(KafkaActionSubscription):
    def __init__(self):
        KafkaActionSubscription.__init__(self, topic='summarizer-route', implementation=ResultParser)

if __name__ == '__main__':
    logging.info('Beginning capture-crawler initialisation')
    runner = CaptureActionRunner()
    logging.info(f'initialised {runner}')
    runner.main()
