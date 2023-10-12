import json
import asyncio
from aiostomp import AioStomp
from amq_processor import case_status
import sys
import threading
import settings
import logging.config
logging.config.dictConfig(settings.LOGGING)
logger = logging.getLogger("WFOLogger")

STOMP_TOPIC_NAME = "/queue/wfo_gui_notify"

logging.basicConfig(
    format="%(asctime)s - %(filename)s:%(lineno)d - "
    "%(levelname)s - %(message)s",
    level='DEBUG')

async def run():
    logger.info("amq_consumer-Connecting")
    client = AioStomp(settings.STOMP_SERVER_HOST, settings.STOMP_SERVER_PORT, error_handler=report_error)
    logger.info("amq_consumer-Subscribing")
    client.subscribe(settings.STOMP_TOPIC_NAME, handler=on_message_case_status)
    await client.connect(settings.STOMP_SERVER_USER, settings.STOMP_SERVER_PASSWORD)
    logger.info("amq_consumer-Connected")

async def on_message_case_status(frame, message):
    logger.info("amq_consumer-on_message_case_status")
    logger.info(message)
    post_json_data = json.loads(message)
    case_id = post_json_data['case_id']
    logger.info("amq_consumer-case_id = "+str(case_id))
    t = threading.Thread(target=case_status,args=(post_json_data,))
    t.start()
    return True

async def report_error(error):
    logger.error('amq_consumer-report_error:', error)

def main(args):
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run())
    finally:
        loop.run_forever()

if __name__ == '__main__':
    main(sys.argv)
