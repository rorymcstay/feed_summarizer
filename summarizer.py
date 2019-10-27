import logging
import os

from src.main.loader import ResultLoader

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))
#logging.FileHandler('/var/tmp/myapp.log')


logging.info("starting summarizer")

if __name__ == '__main__':
    rl = ResultLoader()
    while True:
        # rl.consumeResults()
        rl.produceObjects()
