import ast
import base64
import json
from psycopg2.pool import ThreadedConnectionPool
import psycopg2 as p
import os
from psycopg2.extras import RealDictCursor
import traceback
from rest_framework.response import Response
# import settings
# import logging.config
# logging.config.dictConfig(settings.LOGGING)
# logger = logging.getLogger("WFOLogger")

# #local imports setup
# from route.settings import LOGGING
# import logging.config
# logging.config.dictConfig(LOGGING)
# logger = logging.getLogger("WFOLogger")
# from route import settings


PG_HOST = str(os.environ.get("PG_HOST"))
PG_PORT = 5432
PG_USER = str(os.environ.get("PG_USERNAME"))
PG_PASS = str(os.environ.get("PG_PASSWORD"))
PG_DB_NAME = "postgres"

##local DB setup
# PG_HOST = "production.aibi-prod-fp.ch-dc-os-gsn-107.k8s.dyn.nesc.nokia.net"
# PG_PORT = 30749
# PG_USER = "postgres"
# PG_PASS = "tJEhoVyggE58DYyD"
# PG_DB_NAME = "postgres"

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s.%(msecs)03d | %(levelname)s | %(filename)s | %(funcName)s | %(lineno)d | %(message)s'

        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
        'generic': {
            'format': '%(asctime)s [%(process)d] [%(levelname)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
            'class': 'logging.Formatter',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'django.request': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
        'django.server': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
'django.db.backends': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
        'webapp': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': True
        }
        , 'WFOLogger': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    }
}
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")

env = 'local'

if os.name != 'nt': #Check not Windows(nt)
    if os.getenv('DATA_CENTER') is not None:
        if os.getenv('KEYCLOAK_APP') is not None:
            if 'staging' in os.getenv('KEYCLOAK_APP'):
                env = os.getenv('DATA_CENTER')+'-dev'
            else:
                env = os.getenv('DATA_CENTER')
    if os.getenv('ENV') is not None:
        env = os.getenv('ENV')
if env != "local":
    import ldap,ldap.sasl

tcp = ThreadedConnectionPool(5, 200, user=PG_USER, password=PG_PASS,
                             host=PG_HOST,
                             port=PG_PORT,
                             database=PG_DB_NAME)

def get_pooled_connection():
    conn = tcp.getconn()
    try:
        cur = conn.cursor()
        cur.execute('SELECT 1')
    except p.OperationalError:
        tcp.putconn(conn, key=None, close=True)
        conn = tcp.getconn()
    finally:
        if (conn):
            cur.close()
    return conn


def return_connection(conn):
    try:
        tcp.putconn(conn)
    except BaseException as e:
        logger.error(traceback.format_exc(), str(e))


def connectFetchJSONWithoutResponse(stmt, stmt_data):
    try:
        con = get_pooled_connection()
        cur = con.cursor(cursor_factory=RealDictCursor)
        cur.execute(stmt, stmt_data)
        if stmt.startswith("insert") or stmt.startswith("update"):
            result = cur.rowcount
            con.commit()
        else:
            result = cur.fetchall()
            if result is None:
                result = []
    except (Exception, p.Error) as error:
        if (con):
            return_connection(con)
            logger.error("Failed to Select/insert record into table", error)
            result = []
    finally:
        # closing database connection.
        if (con):
            cur.close()
            return_connection(con)
            logger.info("PostgreSQL connection is closed")
    return result


def connectFetchJSONWihtoutQueryDataNoResponse(stmt):
    result = ""
    try:
        con = get_pooled_connection()
        cur = con.cursor(cursor_factory=RealDictCursor)
        cur.execute(stmt)
        if stmt.startswith("insert") or stmt.startswith("update") or stmt.startswith("delete"):
            result = cur.rowcount
            con.commit()
        else:
            result = cur.fetchall()
    except (Exception, p.Error) as error:
        logger.error("Failed to Select/insert record into table " + str(error))
        if (con):
            return_connection(con)
    finally:
        # closing database connection.
        if (con):
            cur.close()
            return_connection(con)
    return result


def getRequestJSON(req):
    return json.loads(req.body)  # This for Django Request

def user_exists(username, passwd):
	con = p.connect(database=PG_DB_NAME, user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT)
	cur = con.cursor()
	# cur = self.conn.cursor()
	cur.execute("SELECT username,enabled FROM uam_user WHERE username = %s and password = %s", (username, passwd))
	return cur.fetchone() is not None

def connectUpdationsNormal(stmt, data):
	# con = None
	result=0
	try:
		# con = p.connect(database=database, user=user, password=password, host=host, port=port)
		con = get_pooled_connection()
		cur = con.cursor()
		cur.execute(stmt, data)
		result = cur.rowcount
		con.commit()
	# except (Exception, psycopg2.DatabaseError) as error:
	except Exception as error:
		logger.error(traceback.format_exc())
		if (con):
			return_connection(con)
			print("Failed to insert record into table", error)
	finally:
		# closing database connection.
		if (con):
			cur.close()
			return_connection(con)
			print("PostgreSQL connection is closed")
	return result

def connectNativeArray(stmt):
	# con = p.connect("dbname='"+database+"' user='"+user+"' password='"+password+"' host='"+host+"' port='"+port+"'")
	result = []
	try:
		con = get_pooled_connection()
		cur = con.cursor()
		cur.execute(stmt)
		result = [r[0] for r in cur.fetchall()]
	except BaseException as e:
		if (con):
			return_connection(con)
	finally:
		if (con):
			cur.close()
			return_connection(con)
	return result

def response_wrapper_plain_jsondumps(status, data):
	return Response(json.dumps(data), status=status, content_type='application/json')
	# return Response(data, status=status)

def response_wrapper(status, data):
	return response_wrapper_plain_jsondumps(status, data)

def user_exists_ldap(username, passwd):
	l = ldap.initialize("ldap://ed-p-gl.emea.nsn-net.net")
	jsond = {"status": "success"}
	isUserExits = False
	try:
		l.protocol_version = ldap.VERSION3
		l.set_option(ldap.OPT_REFERRALS, 0)

		criteria = "(uid=" + str(username) + ")"
		base = "ou=people,o=nsn"
		results = l.search_s(base, ldap.SCOPE_SUBTREE, criteria)

		print("resultresultresult")
		print(results)
		if len(results) > 0:
			jsond = {"status": "success"}
			userlog = [result[0] for result in results]
			userlog = ",".join(userlog)
			print(userlog)
			bind = l.simple_bind_s(userlog, passwd)
			print(bind)
			isUserExits = True
		else:
			jsond = {"status": "failed", "error": "Invalid User"}
			isUserExits = False
	except Exception as e:
		logger.error(traceback.format_exc())
		print("ldap_serverldap_serverldap_serverldap_server errorrrrrrr111111111111")
		print(str(e))
		jsond = {"status": "failed", "error": str(e)}
		isUserExits = False
	finally:
		l.unbind()

	con = p.connect(database=PG_DB_NAME, user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT)
	cur = con.cursor()
	# cur = self.conn.cursor()
	cur.execute("SELECT username,enabled FROM uam_user WHERE username = %s", (username,))
	if cur.fetchone() is not None:
		query = "update uam_user set updatedtime='now()' where username='{}'".format(username)
		res = connectFetchJSONWihtoutQueryDataNoResponse(query)
	# else:
	# 	query = "select authorities from authorities where username='" + str(username) + "'"
	# 	roles = connectNativeArray(query)
	# 	if len(roles) > 0:
	# 		query = "insert into users (username,password,enabled,updatedtime,createdtime,modifier) values('{}','','1','now()','now()','Siva')".format(username)
	# 		res = connectFetchJSONWihtoutQueryDataNoResponse(query)

	return isUserExits

def cprint(msg=''):
	print(msg)

def getRequestHeaders(req, strVal):
	token = req.META.get(strVal)  # Django
	if 'token' in req.headers:
		return req.headers.get('token')
	new_token = get_new_token_from_request(req, "token")
	if new_token is not None:
		return new_token
	return token

def get_new_token_from_request(req, strVal):
	encoded = req.COOKIES.get('guikeys', None)
	# cprint("encoded %s" % (encoded,))
	if encoded is not None:
		data = base64.b64decode(encoded).decode("UTF-8")
		data = ast.literal_eval(data)
		if strVal in data:
			return data[strVal]
	return None

def checkKey(dict, key):
	if key in dict:
		return True
	else:
		return False

def token_exists(token):
	# cprint(database)
	con = p.connect(database=PG_DB_NAME, user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT)
	cur = con.cursor()
	# cur = self.conn.cursor()
	query = "SELECT username,token FROM token WHERE token = %s and updatedtime > now() - interval '40 minutes'"
	# cprint(query)
	cur.execute(query, (token,))
	return cur.fetchone() is not None

def connectUpdationsNoResp(stmt, data):
	# con = None
	try:
		# con = p.connect(database=database, user=user, password=password, host=host, port=port)
		con = get_pooled_connection()
		cur = con.cursor()
		cur.execute(stmt, data)
		result = cur.rowcount
		con.commit()
	# except (Exception, psycopg2.DatabaseError) as error:
	except Exception as error:
		logger.error(traceback.format_exc())
		if (con):
			return_connection(con)
			print("Failed to insert record into table", error)
	finally:
		# closing database connection.
		if (con):
			cur.close()
			return_connection(con)
			print("PostgreSQL connection is closed")
	return "Updated"
