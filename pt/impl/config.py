import os
from pathlib import Path

import yaml


try:
    with open(os.getenv("DLD_OSM_PT_CONFIG", Path(__file__).parent.parent / ".config.yaml"), "r") as f:
        _config = yaml.safe_load(f)
except:
    _config = {}


CONFIG = _config

ENABLE_CACHE = CONFIG.get("general", {}).get("enable_cache", True)
ENABLE_OVERPASS_CACHE = CONFIG.get("general", {}).get("enable_overpass_cache", True)

PROXIES = CONFIG.get("general", {}).get("proxies", {})

PLAYWRIGHT_CDP_URL = CONFIG.get("playwright", {}).get("cdp_url")
PLAYWRIGHT_CONTEXT_OPTS = CONFIG.get("playwright", {}).get("context_opts", {})
