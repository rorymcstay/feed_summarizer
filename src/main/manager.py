import json
from datetime import datetime
import pandas as pd
import requests as r

from feed.settings import database_parameters, nanny_params
from feed.service import Client
from feed.actiontypes import NeedsMappingWarning

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError

from settings import table_params
import logging


class ObjectManager:
    """
    manages the maps for CaptureAction running, and inserts the results
    into the database.
    """
    batch_size = 10
    max_non_mapped_batches = 3
    def __init__(self, nannyClient):
        logging.info("connecting to database: {}".format(json.dumps(database_parameters, indent=4)))
        dsn = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**database_parameters)
        self.client: Engine = create_engine(dsn)
        self.client.autocommit = True
        self.numberFields = {}
        self.batches = dict()
        self.non_mapped_count = {}
        self.failedBatches = dict()
        self.maps = {}
        self.nannyClient = nannyClient
        try:
            req = r.get("http://{host}:{port}/mappingmanager/getMappingNames/".format(**nanny_params))
            for name in req.json():
                mapping = self.getMapping(name.get('name'))
                if len(mapping) == 0:
                    continue
                else:
                    logging.info(f'have map {mapping} for {name}')
                    self.maps.update({f'{name.get("userID")}:{name.get("userID")}': mapping})
        except Exception as ex:
            logging.warning(f'Failed to communicate with nanny for actionchain details {ex.args}')

    def getMapping(self, name):
        req = self.nannyClient.get(f'/mappingmanager/getMapping/{name}/v/1', resp=True, error={})
        return req.get('value', {'mapping': []}).get('mapping')

    def get_map_key(self, name):
        return f'{self.nannyClient.behalf}:{name}'

    def hasMap(self, name):
        if self.maps.get(self.get_map_key(name)):
            return True
        else:
            return False

    def updateMaps(self, name):
        mapping = self.getMapping(name)
        self.maps.update({self.get_map_key(name): mapping})

    def tryMap(self, name, row):
        mapping = self.maps.get(self.get_map_key(name))
        mapIt = map(lambda col: {col.get('final_column_name'): row.get(col.get('staging_column_name'))}, mapping)
        out = {}
        for mapped in list(mapIt):
            out.update(mapped)
            out.update({'url': row.get('url'), 'added': row.get('')})
        return out

    def _generateTable(self, name, number_fields):
        """
        unused
        """
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

    def updateClients(self, chainName, userID):
        # this method is superfulous as it is done on initialisation already
        self.nannyClient.behalf = userID
        self.nannyClient.chainName = chainName

    def prepareRow(self, name: str, row: dict) -> None:
        """
        take a row and add it to the current batch

        :param name: captureName, the name of the feed.
        :param row: name value pairs of column names to value.
        :return:
        """
        if name not in self.batches.keys():
            # check if we have a batch for this capture, 
            # if not make an empty one.
            self.batches.update({name: pd.DataFrame()})
        if self.hasMap(name):
            # if we have a map, use it.
            row = self.tryMap(name, row)
        elif self.non_mapped_count.get(name, 0) <= (self.max_non_mapped_batches * self.batch_size):
            # if no map, and within non mapped limit increment the non mapped count
            self.non_mapped_count[name] = self.non_mapped_count.get(name, 0) + 1
        else:
            # otherwise, raise needs mapping warning.
            try:
                # this nanny functionality no longer needed with the way we now have defined exceptions.
                self.nannyClient.get(f'/mappingmanager/setNeedsMapping/{name}')
            except Exception as ex:
                pass
            raise NeedsMappingWarning(message='You must set a mapping to continue capturing data.')
        # add the row to the batch
        self.batches[name] = self.batches[name].append(row, ignore_index=True)
        logging.debug("row prepared: {}".format(row))

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

    def insertBatch(self, name: str, sizeCheck: bool = True, retry: bool = False) -> None:
        """
        insert batch into database if batch is ready or batchCheck is false

        :param name: feed name
        :param batchCheck: should the batch be checked for size?
        :return:
        """
        if sizeCheck and len(self.batches[name]) < self.batch_size:
            return
        if not retry:
            self.prepareBatch(name)
        try:
            logging.info(f'inserting batch of size {len(self.batches[name])} for {name}')
            self.batches[name].to_sql(self.getTableName(name),
                                      con=self.client,
                                      if_exists='append')
        except ProgrammingError as e:
            logging.info(f'failed inserting batch {name}')
            if self.handleFailedBatch(name, e):
                self.insertBatch(name, retry=True, sizeCheck=False)
        logging.info(f'Succesfully inserted batch for {name}')
        self.batches.pop(name, None)
        self.updateMaps(name)


    def getTableName(self, name: str) -> str:
        """
        get the table name for feed name

        :param name: the name of the feed
        :return: the table name this process is inserting to
        """
        prefix = table_params.get('fctprefix') if self.hasMap(name) else table_params.get('stgprefix')
        return 't_{prefix}_{name}_{type}{postfix}'.format(name=name, prefix=prefix, **table_params)

    def handleFailedBatch(self, name, e, attempts=0):
        """
        Handle failed batch when the column is not defined in the database, whereby the error is parsed and the missing
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
