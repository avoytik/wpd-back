import requests
import time
import random

COUNTRIES = ["DE", "UK", "FR", "LU"]

class RandomLocation:
  def __init__(self):
    print("Genreating random locations...")

    for c in COUNTRIES:
      for i in range(10):
        response = requests.get(f"https://api.3geonames.org/?randomland={c}&json=1")
        json = response.json()
        self.randomLocations.append({"lat": json["nearest"]["latt"], "lon": json["nearest"]["longt"]})
  def getRandomLocation(self):
    return random.choices(self.randomLocations)[0]
