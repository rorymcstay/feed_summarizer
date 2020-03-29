from feed.logger import getLogger
import os
import argparse
from src.main.loader import ResultLoader

logging = getLogger('summarizer.log')

logging.info(f'starting summarizer')

argparser = argparse.ArgumentParser("Transform data from kafak feed to flatten structure")

argparser.add_argument('--produceObjects', action='store_true', default=False)

if __name__ == "__main__":
    rl = ResultLoader()
    args = argparser.parse_args()
    if args.produceObjects:

        logging.info(f'starting producing objects')
        rl.produceObjects()
        logging.info(f'Goodbye...')
