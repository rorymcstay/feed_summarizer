import requests as r
from feed.settings import nanny_params
from feed.logger import getLogger

logging = getLogger(__name__)

class PathManager:

    def __init__(self):
        self.maps = {}
        req = r.get("http://{host}:{port}/actionsmanager/getActionChains/".format(**nanny_params))
        for name in req.json():
            mapping = self.getMapping(name)
            if len(mapping) == 0:
                continue
            else:
                logging.info(f'have map {mapping} for {name}')
                self.maps.update({name: mapping})

    def getMapping(self, name):
        req = r.get("http://{host}:{port}/mappingmanager/getMapping/{name}/v/1".format(name=name,**nanny_params))
        if req.status_code == 404:
            return []
        return req.json().get('value').get('mapping')

    def hasMap(self,name):
        if self.maps.get(name):
            return True
        else:
            return False

    def updateMaps(self, name):
        mapping = self.getMapping(name)
        self.maps.update({name: mapping})

    def tryMap(self, name, row):
        mapping = self.maps.get(name)
        mapIt = map(lambda col: {col.get('final_column_name'): row.get(col.get('staging_column_name'))}, mapping)
        out = {}
        for mapped in list(mapIt):
            out.update(mapped)
            out.update({'url': row.get('url'), 'added': row.get('')})
        return out
