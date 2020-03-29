import json
import os

import requests
from time import sleep

from kafka import KafkaConsumer, KafkaProducer
from feed.logger import getLogger
from settings import subscribe_params
from feed.settings import retry_params, kafka_params, nanny_params
from kafka.errors import NoBrokersAvailable
from flask import Response
from flask_classy import FlaskView

from src.main.manager import ObjectManager
from src.main.parser import ResultParser

logging = getLogger('summarizer', toFile=True)


class ResultLoader(FlaskView):
   # cacheManager = CacheManager()
    objectManager = ObjectManager()
    markets = f'.*-{subscribe_params["topics"]}'

    producer = KafkaProducer(**kafka_params, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    def __init__(self, attempts=0):
        try:
            self.running = False
            self.kafkaProducer = KafkaProducer(**kafka_params)
            self.kafkaConsumer = KafkaConsumer(**kafka_params)
            logging.warning(f'ResultLoader  is connected to kafka {json.dumps(kafka_params, indent=4)} ')
        except NoBrokersAvailable as e:
            if attempts < retry_params.get("retry_params"):
                sleep(retry_params.get("times"))
                attempts += 1
                logging.info(f'Result loader  is retyring to connect to kafka for the {attempts} time ')
                self.__init__(attempts=attempts)
            else:
                raise e

    def _loadParams(self, params, feedName):

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
                params = self._loadParams(params, feed)
            value = ResultParser(source=message.value, params=params.get(feed)).parseResult()
            item = {"url": value["url"], "type": feed}
            self.producer.send(topic="worker-queue", value=item)
        # self.cacheManager.insertResult(name="{}-results".format(feed), result=value, key=message.key)

    def getSampleData(self, name):
        if name == "undefined":
            return Response(json.dumps(["select a feed"]), mimetype='application/json')
        self.kafkaConsumer.subscribe(pattern=f'{name}-{subscribe_params["topics"]}')
        logging.info(f'subscribed to {self.kafkaConsumer.subscription()}')
        params = {}
        out = []
        size_of_results = 5
        try:
            for message in self.kafkaConsumer:
                logging.debug(message.value)

                feed = message.topic.split("-")[0]
                if params.get(feed) is None:
                    params = self._loadParams(params, feed)
                out.append(str(message.value))
                if len(out) >= 5:
                    logging.info(f'sampled {len(out)} results from {name}')
                    break
        except ValueError as ex:
            # likely iterator already running
            logging.warning(f'ValueError while sampling {name}: {ex.args}')
            return Response(json.dumps({"running": True}), mimetype='application/json')
        return Response(json.dumps(out), status=200, mimetype='application/json')

    def produceObjects(self):
        logging.info(f'subscribing to markets: {self.markets}')
        self.kafkaConsumer.subscribe(pattern=self.markets)
        params = {}
        collected = 0
        for message in self.kafkaConsumer:
            self.running = True
            feed = message.topic.split("-")[0]
            if params.get(feed) is None:
                params = self._loadParams(params, feed)
            row = ResultParser(params=params.get(feed), source=message.value).parseRow()
            self.objectManager.prepareRow(name=feed, row=row)
            logging.info(f'parsed object number: {collected}')
            self.objectManager.insertBatch(name=feed)
            collected += 1
        self.objectManager.insertBatch(name=feed,  sizeCheck=False)
        self.running = False
        logging.info('finished processing')
        return Response(json.dumps({"collected": collected}), status=200, mimetype='application/json')
