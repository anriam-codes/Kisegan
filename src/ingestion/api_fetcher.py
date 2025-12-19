import requests
import yaml
from pathlib import Path

def load_config():
    config_path = Path(__file__).parents[2] / "config" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def fetch_weather():
    config = load_config()

    api_key = config["api"]["api_key"]
    base_url = config["api"]["base_url"]
    city = config["city"]
    fields = ",".join(config["fields"])

    params = {
        "location": f"{city['lat']},{city['lon']}",
        "apikey": api_key,
        "fields": fields,
        "units": "metric"
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()
    return response.json()  # RAW PAYLOAD

if __name__ == "__main__":
    raw_event = fetch_weather()
    print(raw_event)
