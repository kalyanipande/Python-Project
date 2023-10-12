import json, time, stomp
from amq_processor import case_status
import traceback
import settings
import threading
import logging.config
logging.config.dictConfig(settings.LOGGING)
logger = logging.getLogger("WFOLogger")


_queue = "/queue/wfo_gui_notify"
# Subscription id is unique to the subscription in this case there is only one subscription per connection
_sub_id = 1
_reconnect_attempts = 0
_max_attempts = 10

def connect_and_subscribe(conn):
    global _reconnect_attempts
    _reconnect_attempts = _reconnect_attempts + 1
    if _reconnect_attempts <= _max_attempts:
        try:
            conn.connect(settings.STOMP_SERVER_USER, settings.STOMP_SERVER_PASSWORD, wait=True)
            logger.info('amq_subscriber-connect_and_subscribe connecting {} to with connection id {} '
                        'reconnect attempts: {}'.format(settings.STOMP_TOPIC_NAME, _sub_id, _reconnect_attempts))
        except Exception as e:
            traceback.print_exc()
            logger.info('amq_subscriber-Exception on disconnect. reconnecting...')
            logger.error(str(e))
            connect_and_subscribe(conn)
        else:
            conn.subscribe(destination=settings.STOMP_TOPIC_NAME, id=_sub_id, ack='client-individual')
            _reconnect_attempts = 0
    else:
        logger.info('amq_subscriber-Maximum reconnect attempts reached for this connection. reconnect attempts: {}'.format(_reconnect_attempts))


def connect_and_unsubscribe(conn):
    logger.info("amq_subscriber-unsubscribing")
    if conn.is_connected():
        conn.unsubscribe(destination=settings.STOMP_TOPIC_NAME, id=_sub_id)

def connect_and_send(conn,json_data):
    logger.info("amq_subscriber-sending message")
    conn.send(body=json.dumps(json_data), destination=settings.STOMP_TOPIC_NAME)

class MqListener(stomp.ConnectionListener):

    def __init__(self, conn):
        self.conn = conn
        self._sub_id = _sub_id
        logger.info('amq_subscriber-MqListener initialized')

    def on_error(self, frame):
        logger.info('amq_subscriber-received an error "%s"' % frame)

    def on_message(self, frame):
        try:
            logger.info("amq_subscriber-on_message_case_status")
            logger.info('amq_subscriber-message body "%s"' % frame.body)
            post_json_data = json.loads(frame.body)
            subscription_id = frame.headers.get('subscription')
            message_id = frame.headers.get('message-id')
            case_id = post_json_data['case_id']
            logger.info("amq_subscriber-case_id {}".format(case_id))
            t = threading.Thread(target=case_status, args=(post_json_data,))
            t.start()
            self.conn.ack(message_id, subscription_id)
            logger.info('amq_subscriber-processed message message_id {}'.format(message_id))
        except Exception as e:
            logger.error("amq_subscriber-Error", str(e))

    def on_disconnected(self):
        connect_and_unsubscribe(self.conn)
        logger.info('amq_subscriber-disconnected! reconnecting...')
        connect_and_subscribe(self.conn)


def initialize_mqlistener():
    # stomp.Connection
    try:
        # stomp.Connection host
        conn = stomp.Connection([(settings.STOMP_SERVER_HOST, int(settings.STOMP_SERVER_PORT))])
        logger.info("amq_subscriber-Connected to AMQ")
        conn.set_listener('', MqListener(conn))
        connect_and_subscribe(conn)
        while True:
            time.sleep(2)
            if not conn.is_connected():
                logger.info('amq_subscriber-Disconnected in loop, reconnecting')
                connect_and_subscribe(conn)
    except Exception as e:
        logger.error("Exception in amq_subscriber",str(e))
        traceback.print_exc()
    finally:
        if (conn):
            connect_and_unsubscribe(conn)


if __name__ == '__main__':
    initialize_mqlistener()
