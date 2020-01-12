import json
import logging
from datetime import datetime
import pandas as pd

from feed.settings import database_parameters

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError

from settings import table_params


class ObjectManager:
    logging.info("connecting to database: {}".format(json.dumps(database_parameters, indent=4)))
    dsn = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**database_parameters)
    client: Engine = create_engine(dsn)
    client.autocommit = True
    numberFields = {}
    batch_size = 100
    batches = dict()
    failedBatches = dict()

    def _generateTable(self, name, number_fields):
        nl = ",\n      "
        self.numberFields.update({name: number_fields})
        fields = map(lambda number: f'field_{number} VARCHAR(256)', range(number_fields))
        definition = f"""
        CREATE TABLE IF NOT EXISTS {self.getTableName(name)}(
            url VARCHAR(256) PRIMARY KEY,
            added TIMESTAMP NOT NULL,
            {nl.join(list(fields))}
        )
        """
        self.client.execute(definition)

    def prepareRow(self, name: str, row: dict) -> None:
        """
        take a row and add it to the current batch

        :param name: name of feed type
        :param row: data dict
        :return:
        """
        if name not in self.batches.keys():
            self.batches.update({name: pd.DataFrame()})
        self.batches[name] = self.batches[name].append(row, ignore_index=True)
        logging.info("row prepared: {}".format(row))

    def prepareBatch(self, name: str):
        """
        prepare the batch for insertion to the database

        @param name: the feed name
        """
        self.batches[name].index = self.batches[name]['url']
        self.batches[name].index.name = 'url'
        del self.batches[name]['url']
        self.batches[name]['added'] = datetime.now()
        self.batches[name] = self.batches[name].drop_duplicates()

    def insertBatch(self, name: str, batchCheck: bool = True, retry: bool = False) -> None:
        """
        insert batch into database if batch is ready or batchCheck is false

        :param name: feed name
        :param batchCheck: should the batch be checked for size?
        :return:
        """
        if batchCheck and len(self.batches[name]) < self.batch_size:
            return
        if not retry:
            self.prepareBatch(name)

        try:
            self.batches[name].to_sql(self.getTableName(name),
                                      con=self.client,
                                      if_exists='append')
        except ProgrammingError as e:
            if self.handleFailedBatch(name, e):
                self.insertBatch(name, retry=True, batchCheck=False)
        self.batches[name] = pd.DataFrame()

    def getTableName(self, name: str) -> str:
        """
        get the table name for feed name

        :param name: the name of the feed
        :return: the table name this process is inserting to
        """
        return 't_{prefix}_{name}_{type}{postfix}'.format(name=name, **table_params)

    def handleFailedBatch(self, name, e, attempts=0):
        """
        Handle failed batch when the column is not defined, whereby the error is parsed and the missing
        tables names are created in `self.getTableName`.

        parses the error

            ' column "card_price" of relation "t_stg_results" does not exist

        :param name: name of feed
        :param e: the exception
        :param attempts: number of tries to take
        :return:
        """
        if attempts > 5:
            self.failedBatches[name] = self.failedBatches[name].append(self.batches[name])
            return False
        table_name = self.getTableName(name)
        # split on 'relation' to get only column names which are not present
        string = e.args[0].split('relation')[0]
        columns_to_add = list(filter(lambda na: na != table_name, list(filter(lambda s: " " not in s, string.split('"')))))
        cols = ",\n".join([f'add column "{col}" text' for col in columns_to_add])
        logging.debug(f'adding columns {columns_to_add} to {table_name}')
        query = f'alter table {table_name}\n    {cols}'

        logging.warning(f'failed batch, adding column {columns_to_add} to {table_name}.')
        self.client.execute(query)
        return True
