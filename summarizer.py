from feed.logger import getLogger
import logging
import os
from src.main.parser import ResultParser
from feed.actionchains import KafkaActionSubscription
from feed.settings import nanny_params, logger_settings_dict
from logging.config import dictConfig


class CaptureActionRunner(KafkaActionSubscription, ObjectManager):
    def __init__(self):
        queue = f'summarizer-route'
        logging.info(f'subscribing to {queue}')
        KafkaActionSubscription.__init__(self, topic=queue, implementation=ResultParser)
        ObjectManager.__init__()

    def onCaptureActionCallback(self, data: ResultParser.Return):
        self.updateClients(chainName=data.chainName, userID=data.userID)
        try:
            self.prepareRow(name=data.action.captureName, row=data.row)
        except NeedsMappingWarning as ex:
            ex.position = action.position
            ex.actionHash = action.getActionHash()
            raise ex
        self.insertBatch(name=action.captureName)

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
