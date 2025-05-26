import os
import json


with open(os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    'logging_config.json'
)) as __logging_config_file:
    LOGGING_CONFIG = json.load(__logging_config_file)

with open(os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    'short_banners.json'
)) as __short_banners_file:
    SHORT_STORE_BANNERS = json.load(__short_banners_file)
