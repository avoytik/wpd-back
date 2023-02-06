import mwxml
import bz2
from urllib.request import urlopen
from hashlib import md5
import os.path

from wikipedia_page_parser import *
from exceptions import *
from wikipedia_csv import *

PAGES_PROCESS_PROGRESS = 10
PAGE_NAMESPACE = 0

class WikipediaWorker:
  def __init__(self, outputQueue, config):
    self.outputQueue = outputQueue
    self.config = config
    self.pageParser = WikipediaPageParser()
    self.wikipediaCsv = WikipediaCsv()

  def processFile(self, fileUrl):
    localFilename = self._getLocalFileName(fileUrl)

    if self._fileExists(localFilename):
      self._processLocalFile(localFilename)
    else:
      self._processRemoteFile(fileUrl, localFilename)

  def _processLocalFile(self, localFilename):
    self.wikipediaCsv.openForRead(localFilename)

    processedPages = 0
    for pageData in self.wikipediaCsv.read():
      self.outputQueue.put({"page": pageData})
      processedPages += 1

      if processedPages > 1 and (processedPages % PAGES_PROCESS_PROGRESS) == 0:
        self.outputQueue.put({"completed": processedPages})
        processedPages = 0

    if processedPages > 1:
      self.outputQueue.put({"completed": processedPages})

    self.wikipediaCsv.close()

  def _processRemoteFile(self, fileUrl, localFilename):
    self.wikipediaCsv.openForWrite(localFilename)

    with urlopen(fileUrl) as stream:
      with bz2.BZ2File(stream) as file:
        dump = mwxml.Dump.from_file(file)
        processedPages = 0
        for page in dump.pages:
          if page.redirect is None and page.namespace == PAGE_NAMESPACE:
            *_, revision = page
            if revision.text is not None:
              if self.pageParser.needToParse(revision.text):
                try:
                  pageData = self.pageParser.parse(page.id, page.title, revision.text)
                  self.outputQueue.put({"page": pageData})
                  self.wikipediaCsv.write(pageData)
                  processedPages += 1
                except InvalidDateException:
                  pass
          if processedPages > 1 and (processedPages % PAGES_PROCESS_PROGRESS) == 0:
            self.outputQueue.put({"completed": processedPages})
            processedPages = 0

        if processedPages > 1:
          self.outputQueue.put({"completed": processedPages})

    self.wikipediaCsv.close()

  def _getLocalFileName(self, fileUrl):
    return f"{os.path.dirname(__file__)}/pages/{md5(fileUrl.encode()).hexdigest()}.csv"

  def _fileExists(self, localFilename):
    return os.path.isfile(localFilename)
