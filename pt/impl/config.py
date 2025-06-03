import os
from pathlib import Path

import yaml


try:
    with open(os.getenv("DLD_OSM_PT_CONFIG", Path(__file__).parent.parent / ".config.yaml"), "r") as f:
        CONFIG = yaml.safe_load(f)
except:
    CONFIG = {}


ENABLE_CACHE = CONFIG.get("general", {}).get("enable_cache", True)
ENABLE_OVERPASS_CACHE = CONFIG.get("general", {}).get("enable_overpass_cache", True)

PLAYWRIGHT_CDP_URL = CONFIG.get("playwright", {}).get("cdp_url")
PLAYWRIGHT_CDP_CAPTCHA_FOUND = CONFIG.get("playwright", {}).get("cdp_captcha_found")
PLAYWRIGHT_CDP_CAPTCHA_SOLVE = CONFIG.get("playwright", {}).get("cdp_captcha_solve")
PLAYWRIGHT_CONTEXT_OPTS = CONFIG.get("playwright", {}).get("context_opts", {})
