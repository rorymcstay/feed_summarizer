import json
import os

from kafka import KafkaConsumer, KafkaProducer
import logging
from settings import kafka_params
from src.main.manager import ObjectManager
from src.main.parser import ResultParser


class ResultLoader():
   # cacheManager = CacheManager()
    objectManager = ObjectManager()
    kafkaConsumer = KafkaConsumer(**kafka_params)
    markets = f'.*-{kafka_params["topics"]}'

    producer = KafkaProducer(**kafka_params, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def consumeResults(self):
        self.kafkaConsumer.subscribe(pattern=self.markets)
        for message in self.kafkaConsumer:
            feed = message.topic.split("-")[0]
            value = ResultParser(feedName=feed, source=message.value).parseResult()
            item = {"url": value["url"], "type": feed}
            self.producer.send(topic="worker-queue", value=item)
           # self.cacheManager.insertResult(name="{}-results".format(feed), result=value, key=message.key)

    def produceObjects(self):
        logging.info(f'subscribing to markets: {self.markets}')
        self.kafkaConsumer.subscribe(pattern=self.markets)
        for message in self.kafkaConsumer:
            feed = message.topic.split("-")[0]
            row = ResultParser(feedName=feed, source=message.value).parseRow()
            self.objectManager.prepareRow(name=feed, row=row)
            self.objectManager.insertBatch(name=feed)

