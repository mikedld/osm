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

PLAYWRIGHT_CDP_URL = _config.get("playwright", {}).get("cdp_url")
PLAYWRIGHT_CDP_CAPTCHA_FOUND = _config.get("playwright", {}).get("cdp_captcha_found")
PLAYWRIGHT_CDP_CAPTCHA_SOLVE = _config.get("playwright", {}).get("cdp_captcha_solve")
PLAYWRIGHT_CONTEXT_OPTS = _config.get("playwright", {}).get("context_opts", {})
