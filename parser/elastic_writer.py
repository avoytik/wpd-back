import os
from elasticsearch import Elasticsearch, exceptions
from hashlib import md5
from datetime import date

from constants import *
from random_location import *

PAGE_INDEX_CONFIG = {
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "analysis": {
      "analyzer": {
        "autocomplete": {
          "tokenizer": "keyword",
          "filter": [
            "lowercase",
            "asciifolding",
            "word_joiner",
            "autocomplete_filter",
          ],
        },
        "standard_autocomplete": {
          "tokenizer": "keyword",
          "filter": [
            "lowercase",
            "asciifolding",
            "word_joiner",
          ],
        },
      },
      "normalizer": {
        "keyword_normalizer": {
          "type": "custom",
          "char_filter": [],
          "filter": ["lowercase", "asciifolding"],
        },
      },
      "filter": {
        "autocomplete_filter": {
          "type": "edge_ngram",
          "min_gram": 1,
          "max_gram": 20,
          "token_chars": [
            "letter",
            "digit",
          ],
        },
        "word_joiner": {
          "type": "word_delimiter",
          "catenate_all":  True,
        },
      },
    },
  },
  "mappings": {
    "properties": {
      "name": {
        "type": "text",
        "analyzer": "autocomplete",
        "search_analyzer": "standard_autocomplete",
      },
      "birth_date": {
        "type": "date"
      },
      "death_date": {
        "type": "date"
      },
      "birth_place": {
        "type": "text"
      },
      "birth_place_location": {
        "type": "geo_point"
      },
      "text": {
        "type": "text"
      },
      "first_paragraph": {
        "type": "text",
        "analyzer": "autocomplete",
        "search_analyzer": "standard_autocomplete",
      },
    }
  }
}

GEOLOCATION_INDEX_CONFIG = {
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "date": {
        "type": "date"
      },
      "location_geo_point": {
        "type": "geo_point"
      },
    }
  }
}

class ElasticWriter:
  def __init__(self):
    self.client = Elasticsearch(os.getenv("ELASTIC_HOST"), http_auth=(os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD")))
    if self.client.ping():
      print("Connection to elasticsearch established")
    else:
      raise RuntimeError("Could not connect to elasticsearch")
    self.locationRandomizer = RandomLocation()
    self.todaysGeoLocationCalls = 0

  def open(self):
    self._checkIndexes()
#     self._getTodaysGeolocationsCalls()

  def handleEvent(self, evt):
    if "page" in evt:
      try:
        pageId = evt["page"]["page_id"]

        del evt["page"]["page_id"]
        doc = evt["page"]
        if "birth_place" in doc and doc["birth_place"]:
          doc.update({ "birth_place_location": self._getGeoLocation(doc["birth_place"]) })
        self.client.index(index=PAGE_INDEX_NAME, id=pageId, body=doc)
      except exceptions.BadRequestError:
        print("Failed to add page:")
        print(pageId, doc["name"])
        pass

  def _getGeoLocation(self, location):
    locationId = md5(location.encode()).hexdigest()

    try:
      storedLocation = self.client.get(index=GEOLOCATION_INDEX_NAME, id=locationId)

      return storedLocation["_source"]["location_geo_point"] if "location_geo_point" in storedLocation["_source"] else None
    except exceptions.NotFoundError:
      doc = {
        "date": date.today().strftime(YMD_FORMAT),
        "location_geo_point": self.locationRandomizer.getRandomLocation()
      }
      self.client.index(index=GEOLOCATION_INDEX_NAME, id=locationId, body=doc)

      return doc["location_geo_point"]

  def _checkIndexes(self):
    if not self.client.indices.exists(index=PAGE_INDEX_NAME):
      self.client.indices.create(index=PAGE_INDEX_NAME, body=PAGE_INDEX_CONFIG)
    if not self.client.indices.exists(index=GEOLOCATION_INDEX_NAME):
      self.client.indices.create(index=GEOLOCATION_INDEX_NAME, body=GEOLOCATION_INDEX_CONFIG)

#   def _getTodaysGeolocationsCalls(self):
#     self.todaysGeoLocationCalls = 1000000

#   def _isGeoLocationCallLimitReached(self):
#     return self.todaysGeoLocationCalls >= MAX_GEO_LOCATION_CALLS_PER_DAY
