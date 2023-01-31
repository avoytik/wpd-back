import bz2
from urllib.request import urlopen
import json
import re
from constants import WIKIMEDIA_URL
from constants import DUMP_STATUS_URL

class WikipediaDumpStatusRetriever:
  def __init__(self, wikimediaUrl=WIKIMEDIA_URL, dumpStatusUrl=DUMP_STATUS_URL):
    self.wikimediaUrl = wikimediaUrl
    self.dumpStatusUrl = dumpStatusUrl

  def getDumpStatusFiles(self):
    articlesFiles = []

    with urlopen(self.wikimediaUrl + self.dumpStatusUrl) as stream:
      data = json.load(stream)
      for k, v in data['jobs']['articlesmultistreamdump']['files'].items():
        if re.match(r"(.*\.xml.*\.bz2)", k) is not None:
          articlesFiles.append(self.wikimediaUrl +  v['url'])

    return articlesFiles
