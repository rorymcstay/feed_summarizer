import logging
import os
import argparse
from src.main.loader import ResultLoader

argparser = argparse.ArgumentParser("Transform data from kafak feed to flatten structure")

argparser.add_argument('--produceObjects', action='store_true', default=False)

if __name__ == "__main__":
    rl = ResultLoader()
    args = argparser.parse_args()
    if args.produceObjects:
        rl.produceObjects()
