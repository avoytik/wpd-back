from multiprocessing import freeze_support
from wikipedia_process import *
from dotenv import load_dotenv

if __name__ == '__main__':
  load_dotenv()
  freeze_support()
  process = WikipediaProcess()
  process.start()
