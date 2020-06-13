from feed.logger import getLogger
from feed.service import Client
import logging
import os
from src.main.parser import ResultParser
from feed.actionchains import KafkaActionSubscription
from feed.settings import nanny_params, logger_settings_dict
from feed.actiontypes import NeedsMappingWarning
from logging.config import dictConfig
from src.main.manager import ObjectManager


class CaptureActionRunner(KafkaActionSubscription, ObjectManager):
    def __init__(self):
        queue = f'summarizer-route'
        logging.info(f'subscribing to {queue}')
        KafkaActionSubscription.__init__(self, topic=queue, implementation=ResultParser)
        ObjectManager.__init__(self, Client("nanny", **nanny_params))

    def onCaptureActionCallback(self, data: ResultParser.Return, **kwargs):
        self.updateClients(chainName=data.chainName, userID=data.userID)
        try:
            self.prepareRow(name=data.action.captureName, row=data.row)
        except NeedsMappingWarning as ex:
            ex.position = data.action.position
            ex.actionHash = data.action.getActionHash()
            raise ex
        self.insertBatch(name=data.action.captureName)

if __name__ == '__main__':
    dictConfig(logger_settings_dict('root'))

    logging.getLogger('conn').setLevel('WARNING')
    logging.getLogger('urllib').setLevel('WARNING')
    logging.getLogger('parser').setLevel('WARNING')
    logging.getLogger('metrics').setLevel('WARNING')
    logging.getLogger('connectionpool').setLevel('WARNING')
    logging.getLogger('kafka').setLevel('WARNING')
    logging.getLogger('config').setLevel('WARNING')
    logging.info("####### Environment #######")

    logging.info("####### Environment #######")
    logging.info(f'nanny: {nanny_params}')
    logging.info("\n".join([f'{key}={os.environ[key]}' for key in os.environ]))

    logging.info('Beginning capture-crawler initialisation')
    logging.info("\n".join([f'{key}={os.environ[key]}' for key in os.environ]))
    runner = CaptureActionRunner()
    logging.info(f'initialised {runner}')
    runner.main()
