import json
import logging
from datetime import datetime
from sqlalchemy.exc import ProgrammingError

import hazelcast
import pandas as pd
import psycopg2
# from hazelcast import ClientConfig, HazelcastClient
from hazelcast.core import HazelcastJsonValue
from psycopg2._psycopg import connection
from sqlalchemy import create_engine

from settings import database_parameters


class CacheManager:

    def insertResult(self, name, result, key):
        """
        request the next set of results

        :return:
        """
        pass


#       map = self.client.get_map(name)
#       map.put(key=key, value=HazelcastJsonValue(json.dumps(result)))

#       logging.debug('inserted object result {}'.format(key))


class ObjectManager:
    logging.info("connecting to database: {}".format(json.dumps(database_parameters, indent=4)))
    dsn = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**database_parameters)
    client: connection = create_engine(dsn)
    client.autocommit = True
    numberFields = {}
    batch_size = 100

    batches: pd.DataFrame = {}

    def _generateTable(self, name, number_fields):
        self.numberFields.update({name: number_fields})
        fields = map(lambda number: "field_{} VARCHAR(256)".format(number), range(number_fields))
        definition = """
        CREATE TABLE IF NOT EXISTS t_stg_{name}_results_{number_fields}(
            url VARCHAR(256) PRIMARY KEY,
            added TIMESTAMP NOT NULL,
            {fields}
        )
        """.format(fields=",\n      ".join(list(fields)), name=name, number_fields=number_fields)
        c = self.client.cursor()
        c.execute(definition)

    def prepareRow(self, name, row):
        if name not in self.batches.keys():
            self.batches.update({name: pd.DataFrame()})

        self.batches[name] = self.batches[name].append(row, ignore_index=True)
        logging.info("row prepared: {}".format(row))

    def insertBatch(self, name, batchCheck=True):
        if (batchCheck and len(self.batches[name]) < self.batch_size):
            return
        self.batches[name] = self.batches[name].set_index(['url'])
        self.batches[name]['added'] = datetime.now()
        try:
            self.batches[name].to_sql('t_stg_{}_results{appendix}'.format(name,
                                                                          appendix=database_parameters.get(
                                                                              "appendix")),
                                      con=self.client,
                                      if_exists='append')
        except ProgrammingError as e:
            self.handleFailedBatch(name, e)
            self.insertBatch(name)
        self.batches[name] = pd.DataFrame()

    def handleFailedBatch(self, name, e):
        table_name = 't_stg_{}_results{appendix}'.format(name, appendix=database_parameters.get("appendix"))
        c = self.client.cursor()
        string = e.args[0]
        columns_to_add = list(filter(lambda: name == table_name, list(filter(lambda s: " " not in s, string.split('"')))))
        cols = ",\n".join([f'add column {col} text' for col in columns_to_add])
        query = f'alter table {table_name} \n {cols}'
        logging.warning(f'failed batch, adding column {columns_to_add} to {table_name}')
        c.execute(query)
        c.commit()
