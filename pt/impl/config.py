import os
from pathlib import Path

import yaml


try:
    with open(os.getenv("DLD_OSM_PT_CONFIG", Path(__file__).parent.parent / ".config.yaml"), "r") as f:
        _config = yaml.safe_load(f)
except:
    _config = {}


ENABLE_CACHE = _config.get("general", {}).get("enable_cache", True)
ENABLE_OVERPASS_CACHE = _config.get("general", {}).get("enable_overpass_cache", True)

SCRAPERAPI_API_KEY = _config.get("scraperapi", {}).get("api_key")

PLAYWRIGHT_CONTEXT_OPTS = _config.get("playwright", {}).get("context_opts", {})
