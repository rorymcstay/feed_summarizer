import json
import os

import requests
from time import sleep

from kafka import KafkaConsumer, KafkaProducer
import logging
from settings import subscribe_params
from feed.settings import retry_params, kafka_params, nanny_params
from kafka.errors import NoBrokersAvailable

from src.main.manager import ObjectManager
from src.main.parser import ResultParser


class ResultLoader():
   # cacheManager = CacheManager()
    objectManager = ObjectManager()

    markets = f'.*-{subscribe_params["topics"]}'

    producer = KafkaProducer(**kafka_params, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    def __init__(self, attempts=0):
        try:
            self.kafkaProducer = KafkaProducer(**kafka_params)
            self.kafkaConsumer = KafkaConsumer(**kafka_params)
        except NoBrokersAvailable as e:
            if attempts < retry_params.get("retry_params"):
                sleep(retry_params.get("times"))
                attempts += 1
                logging.info(f'Result loader  is retyring to connect to kafka for the {attempts} time ')
                self.__init__(attempts=attempts)
            else:
                raise e

    def loadParams(self, params, feedName):

        r = requests.get(
            "http://{host}:{port}/parametercontroller/getParameter/summarizer/{name}".format(**nanny_params,
                                                                                             name=feedName))
        params.update({feedName: r.json()})
        return params

    def consumeResults(self):
        self.kafkaConsumer.subscribe(pattern=self.markets)
        logging.info(f'subscribed to {self.kafkaConsumer.subscription()}')
        params = {}
        for message in self.kafkaConsumer:
            feed = message.topic.split("-")[0]
            if params.get(feed) is None:
                params = self.loadParams(params, feed)
            value = ResultParser(source=message.value, params=params.get(feed)).parseResult()
            item = {"url": value["url"], "type": feed}
            self.producer.send(topic="worker-queue", value=item)
        # self.cacheManager.insertResult(name="{}-results".format(feed), result=value, key=message.key)

    def produceObjects(self):
        logging.info(f'subscribing to markets: {self.markets}')
        self.kafkaConsumer.subscribe(pattern=self.markets)
        params = {}
        for message in self.kafkaConsumer:
            feed = message.topic.split("-")[0]
            if params.get(feed) is None:
                params = self.loadParams(params, feed)
            row = ResultParser(params=params.get(feed), source=message.value).parseRow()
            self.objectManager.prepareRow(name=feed, row=row)
            self.objectManager.insertBatch(name=feed)
