import multiprocessing as mp
import queue
import time
import traceback

from wikipedia_dump_status import *
from utils import *
from wikipedia_worker import *
from elastic_writer import *
from constants import *

class WikipediaProcess:
  def __init__(self):
    self.dumpStatusRetriever = WikipediaDumpStatusRetriever()
    self.elasticWriter = ElasticWriter()
    self.dumpStatusFiles = []
    self.workersRunning = 0
    self.workers = 0
    self.totalCount = 0
    self.processedFiles = 0
    self.lastUpdate = 0
  def start(self):
    startTime = time.time()

    self.elasticWriter.open()

    print("Downloading dump status")
    self.dumpStatusFiles = self.dumpStatusRetriever.getDumpStatusFiles()

    print(f"Processing {len(self.dumpStatusFiles)} files")
    cpus = mp.cpu_count()
    print(f"Detected {cpus} cores.")
    self.workers = min(MAX_THREADS, int(cpus * .5))
    print(f"Using {self.workers} threads")

    inputQueue = mp.Queue()
    outputQueue = mp.Queue(QUEUE_SIZE)

    processes = []
    for i in range(self.workers):
      config = {
        'num': i+1
      }

      p = mp.Process(target=WikipediaProcess.worker, args=(inputQueue, outputQueue, config))
      p.start()
      p.name = f"process-{i}"
      processes.append(p)

    self.workersRunning = self.workers

    for file in self.dumpStatusFiles:
      inputQueue.put(file)

    for i in range(self.workers*2):
      inputQueue.put("**exit**")

    error_exit = False
    while (self.processedFiles < len(self.dumpStatusFiles)) and not error_exit:
        evt = self.getEvent(outputQueue)
        self.handleEvent(evt)
        self.elasticWriter.handleEvent(evt)

    print(f"pages parsed: {self.totalCount:,}; files: {self.processedFiles}/{len(self.dumpStatusFiles)}")
    self.shutdown(processes, outputQueue)
    inputQueue.close()
    outputQueue.close()

    elapsedTime = time.time() - startTime
    print("Elapsed time: {}".format(hmsString(elapsedTime)))
    print("Done...")

  def shutdown(self, processes, eventQueue):
    done = False
    print("waiting for workers to write remaining results")
    while not done:
      done = True
      for p in processes:
        p.join(10)
        if p.exitcode is None:
          done = False
        try:
          evt=eventQueue.get(timeout=10)
          self.handleEvent(evt)
        except queue.Empty:
          pass

  def handleEvent(self, evt):
    if "completed" in evt:
      self.totalCount += evt["completed"]
      self.currentUpdate = int(self.totalCount / PAGES_PROGRESS_THRESHOLD)
      if self.currentUpdate != self.lastUpdate:
        print(f"pages parsed: {self.currentUpdate * PAGES_PROGRESS_THRESHOLD:,}; files: {self.processedFiles}/{len(self.dumpStatusFiles)}, workers: {self.workersRunning}/{self.workers}")
        self.lastUpdate = self.currentUpdate
    elif "file_complete" in evt:
      self.processedFiles += 1
    elif "**worker done**" in evt:
      self.workersRunning -= 1
      print(f"Worker done: {evt['**worker done**']}")

  @staticmethod
  def getEvent(eventQueue):
    isDone = False
    retry = 0
    while not isDone:
      try:
        return eventQueue.get(timeout=GET_EVENT_TIMEOUT)
      except queue.Empty:
        retry += 1
        if retry <= GET_EVENT_MAX_RETRY:
          print(f"Queue get timeout, retry {retry}/{GET_EVENT_MAX_RETRY}")
        else:
          print(f"Queue timeout failed, retry {GET_EVENT_MAX_RETRY} failed, exiting.")
          isDone = True
          return None

  @staticmethod
  def worker(inputQueue, outputQueue, config):
    try:
      worker = WikipediaWorker(outputQueue, config)
      done = False

      while not done:
        path = inputQueue.get()
        if path != "**exit**":
          try:
            worker.processFile(path)
          except Exception as e:
            print(f"Url: {path}. Error: {e}")
            traceback.print_exc()
          finally:
            outputQueue.put({"file_complete": True})
        else:
          done = True
    finally:
        outputQueue.put({"**worker done**": config['num']})
