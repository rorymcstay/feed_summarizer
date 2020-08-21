import re

import bs4
import requests
from bs4 import Tag, NavigableString
from feed.actionchains import CaptureAction
from feed.logger import getLogger
from feed.settings import nanny_params
from feed.actionchains import ActionChain
from feed.actiontypes import CaptureAction
from feed.actiontypes import NeedsMappingWarning

logging = getLogger('summarizer', toFile=True)

class Path:
    def __init__(self):
        pass
    def getName():
        pass
    def addStep(step: str, container):
        if container == "LIST":
            pass


class ResultParser(ActionChain):


    def __init__(self, driver, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.backupKeyIncrement = 0


    def parseResult(self):
        items = {}
        self.params.pop("name")
        for item in self.params:
            items.update(self.getItem(item, self.soup))
        return items

    def selectPath(path):
        pass

    def getItem(self, item: str, start: Tag) -> dict:
        """
        This function assumes it is starting from a single result node extracted from a list
        :param item: the name of the item to get in the settings/results dict
        :param start: the node to start from
        :return:
        """
        path = self.params[item]

        if path['attr'] and path['single']:
            for step in path['class']:
                startTent = start.findAll(attrs={"class": step})
                if len(startTent) > 0:
                    start = startTent[0]
            try:
                # if this doesnt work get the children... eg a photo tag
                finish = start.attrs[path['attr']]
            except:
                finish = [node if isinstance(node, NavigableString) or node is None else node.attrs.get(path['attr'])
                          for node in start.children]
        elif path['attr'] and not path['single']:
            for step in path['class'][:-1]:
                start = start.findAll(attrs={"class": step})[0]
            if len(path['class']) > 0:
                start.findAll(attrs={"class": path["class"][-1]})
            finish = [sta.attrs.get(path['attr']) if isinstance(sta, Tag) else sta for sta in start]
        # attrs will not come past here
        elif path['single']:
            for node in path["class"]:
                start = start.find(attrs={"class": node})
            finish = start.text if start is not None else None
        else:
            for node in path["class"]:
                start = start.find(attrs={"class": node})
            if start is not None:
                finish = [node if isinstance(node, NavigableString) or node is None else node.text for node in
                          start.children]
            else:
                finish = None
        return {item: finish}

    def _traverse(self, child: Tag, fields: dict, images: dict, fromClass=None):
        """
        recursively flatten a html structure into key value pairs based on tag name.
        :param: child: the BeautifulSoup representation of html TODO: handle NavigableString and string.
        :param: fields: empty dict of datafields
        :param: images: empty dict of images
        :param: fromClass: only used on recursive calls, the class html to search recusively is called.

        TODO: We should build up a path here incase it isuseful later.

        """
        if isinstance(child, Tag):
            if child.name == 'img':
                images.update({"image_".format(len(images) + 1): child.attrs.get("src")})
                return
            thisClass = child.attrs.get("class")
            if thisClass is not None:
                fromClass = thisClass
            if isinstance(fromClass, list):
                fromClass = "-".join(fromClass)
            for item in child.children:
                self._traverse(item, fields, images, fromClass)
                if item.next_sibling is None:
                    return
        count = 0
        for key in fields:
            if key.startswith(fromClass):
                count += 1
        if count > 0:
            fromClass = "{}_{}".format(fromClass, count + 1)
        try:
            # will break if not Tag, if str should take that.
            text = child.text
        except:
            text = ''
        if text:
            field = child.text
        else:
            field = str(child).strip(' \n')
        if field and field.strip() != '':
            fields.update({fromClass: field})

    class Return:
        """
        Return type for the capture action.
        """
        def __init__(self, row: dict, action: CaptureAction, chainName: str, userID: str):
            self.action=action
            self.row=row
            self.chainName=chainName
            self.userID = userID

    def onCaptureAction(self, action: CaptureAction):
        logging.info(f'ResultParser::onCaptureAction()')
        self.soup = bs4.BeautifulSoup(action.data[0], "html.parser")
        fields = {}
        images = {}
        # TODO should use bs4 here instead
        # get all the links in a list
        index = re.findall(r'href="[^\"]+', str(self.soup))
        logging.info(f'ResultParser::onCaptureAction(): have potentialUrls=[{index}]')
        url=None
        if len(index) != 0:
            url = index[0].split("=")[1].strip('"')
        else:
            logging.info(f'{type(self).__name__}::onCaptureAction(): failed to find urls.')
        for child in self.soup:
            self._traverse(child, fields, images)
        if url is None:
            # we use the back up url
            self.backupKeyIncrement += 1
            logging.warning(f'No valid url found for {action}, using parent tag meta')
            url = f'{action.data[1].get("href", action.backupKey)}-{self.backupKeyIncrement}'
            logging.info(f'Using {url} for {action}, taken from parent')
        fields.update({'url': url})
        fields.update(images)
        return [ResultParser.Return(row=fields, action=action, chainName=self.name, userID=self.userID)]

