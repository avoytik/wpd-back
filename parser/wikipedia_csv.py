import codecs
import csv
import sys
import ctypes as ct

ENCODING = "UTF-8"

class WikipediaCsv:
  def __init__(self):
    self.fp = None
    self.writer = None
    self.reader = None
    csv.field_size_limit(int(ct.c_ulong(-1).value // 2))

  def openForWrite(self, fileName):
    self.fp = codecs.open(fileName, "w+", ENCODING)
    self.writer = csv.writer(self.fp, quoting=csv.QUOTE_MINIMAL)
    self.reader = None

  def openForRead(self, fileName):
    self.fp = codecs.open(fileName, "r", ENCODING)
    self.reader = csv.reader(self.fp, quoting=csv.QUOTE_MINIMAL)
    self.writer = None

  def close(self):
    self.fp.close()
    self.fp = None
    self.writer = None
    self.reader = None

  def write(self, data):
    if self.fp is None or self.writer is None:
      raise RuntimeError("File is not opened for write")

    self.writer.writerow(self._prepareData(data))

  def read(self):
    if self.fp is None or self.reader is None:
      raise RuntimeError("File is not opened for read")

    for row in self.reader:
      yield {
        "page_id": row[0],
        "text": row[1],
        "first_paragraph": row[2],
        "name": row[3],
        "birth_date": row[4] if row[4] else None,
        "death_date": row[5] if row[5] else None,
        "birth_place": row[6] if row[6] else None
      }

  def _prepareData(self, data):
    return [
      data["page_id"],
      data["text"],
      data["first_paragraph"],
      data["name"],
      data["birth_date"] if "birth_date" in data and data["birth_date"] else "",
      data["death_date"] if "death_date" in data and data["death_date"] else "",
      data["birth_place"] if "birth_place" in data and data["birth_place"] else ""
    ]
