import requests
import json
from route import settings
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
import traceback


class Lexicon_updater():
    def schedule_lexicon_api(self):
        post_data = {
            "userId": "sumitg",
            "requestId": "4567",
        }
        json_data = json.dumps(post_data)
        logger.info("Calling DE Lexicon API")
        response = requests.post(settings.POST_LEXICON_URL, data=json_data)
        if response.status_code == 200 and response.json().get('status') == '200':
            logger.info("lexicon schedule api started")
        else:
            logger.info("not able to start lexicon api")