import requests
import yaml
import time
from pathlib import Path

def load_config():
    path = Path(__file__).parents[2] / "config" / "config.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)

def fetch_weather_for_location(location, api_key, base_url):
    params = {
        "lat": location["lat"],
        "lon": location["lon"],
        "appid": api_key,
        "units": "metric"
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()

    return {
        "location": {
            "name": location["name"],
            "lat": location["lat"],
            "lon": location["lon"]
        },
        "data": {
            "observationTime": data.get("dt"),
            "temperature": data["main"].get("temp"),
            "temperatureMin": data["main"].get("temp_min"),
            "temperatureMax": data["main"].get("temp_max"),
            "humidity": data["main"].get("humidity"),
            "pressureSurfaceLevel": data["main"].get("pressure"),
            "windSpeed": data["wind"].get("speed"),
            "windDirection": data["wind"].get("deg"),
            "cloudCover": data["clouds"].get("all"),
            "visibility": data.get("visibility")
        }
    }

def fetch_all_locations():
    config = load_config()
    api_key = config["api"]["api_key"]
    base_url = config["api"]["base_url"]
    locations = config["locations"]

    events = []
    for loc in locations:
        events.append(fetch_weather_for_location(loc, api_key, base_url))
        time.sleep(3)  # rate-limit safety

    return events

if __name__ == "__main__":
    for e in fetch_all_locations():
        print(e)