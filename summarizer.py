from feed.logger import getLogger
import os
import argparse
from src.main.parser import ResultParser
from feed.actionchains import KafkaActionSubscription

logging = getLogger(__name__)

logging.info(f'starting summarizer')

argparser = argparse.ArgumentParser("Transform data from kafak feed to flatten structure")

argparser.add_argument('--produceObjects', action='store_true', default=False)

argparser.add_argument('--headlessMode', action='store_true', default=False)

class CaptureActionRunner(KafkaActionSubscription):
    def __init__(self):
        KafkaActionSubscription.__init__(self, topic='summarizer-route', implementation=ResultParser)

if __name__ == '__main__':
    runner = CaptureActionRunner()
    runner.main()
