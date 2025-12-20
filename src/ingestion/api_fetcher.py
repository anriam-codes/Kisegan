import requests
import yaml
from pathlib import Path

def load_config():
    path = Path(__file__).parents[2] / "config" / "config.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)

def fetch_weather_for_location(location, api_key, base_url, fields):
    params = {
        "location": f"{location['lat']},{location['lon']}",
        "apikey": api_key,
        "fields": ",".join(fields),
        "units": "metric"
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()

    data = response.json()
    data["location"]["name"] = location["name"]  # attach area name
    return data  # RAW JSON

def fetch_all_locations():
    config = load_config()
    api_key = config["api"]["api_key"]
    base_url = config["api"]["base_url"]
    locations = config["locations"]
    fields = config["fields"]

    events = []
    for loc in locations:
        event = fetch_weather_for_location(loc, api_key, base_url, fields)
        events.append(event)

    return events

if __name__ == "__main__":
    events = fetch_all_locations()
    for e in events:
        print(e)
