import MySQLdb
import traceback
import json
from rest_framework.response import Response
import logging.config
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
from django.db import connection
import numpy as np
def connectFetchfrompostgreaslist(stmt):
    try:
        cursor = connection.cursor()
        cursor.execute(stmt)
        # row = cursor.fetchone()
        logger.info(stmt)
        result = list(cursor.fetchall())
        connection.close()
        final_result = [i[0] for i in result]
        clean_result = [i for i in final_result if i != None and i != " " and i != np.nan and i != "nan"]
        resp = response_wrapper(200, clean_result)
    except Exception as e:
        logger.error(traceback.format_exc())
        resp = response_wrapper(500, str(e))
    return resp


def FetchFromPostGres(stmt):
    try:
        cursor = connection.cursor()
        cursor.execute(stmt)
        logger.info(stmt)
        result = list(cursor.fetchall())
        print("res :", result)
        keys = ['case_id','case_id_new','created_at','input_text','modified_at','status','tag','release']
        final_result=list()
        for res in result:
            res_dict = dict()
            for i in range(len(keys)):
                res_dict[keys[i]] = res[i]
            final_result.append(res_dict)
        # final_result = [i for i in result]
        resp = response_wrapper(200, final_result)
    except Exception as e:
        logger.error(traceback.format_exc())
        resp = response_wrapper(500, str(e))
    return resp
def getRequestJSON(req):
    return json.loads(req.body)

def response_wrapper(status, data):
    json_data = {}
    if (status == 200):
        res_msg = "Data Retrieved"
        res_status = 200
        res_error = ''
        json_data = {'data': data, 'msg': res_msg, 'status': res_status, 'error': res_error}
    elif (status == 500):
        res_msg = "Internal Server Error"
        res_status = 500
        res_error = str(data)
        json_data = {'data': [], 'msg': res_msg, 'status': res_status, 'error': res_error}
    elif (status == 401):
        res_msg = "Invalide Credentials"
        res_status = 401
        res_error = ''
        json_data = {'data': data, 'msg': res_msg, 'status': res_status, 'error': res_error}
    elif (status == 4001):
        res_msg = "Auth Token Not Found"
        res_status = 401
        res_error = ''
        json_data = {'data': data, 'msg': res_msg, 'status': res_status, 'error': res_error}
    elif (status == 4002):
        res_msg = "Invalid Token/Expired"
        res_status = 401
        res_error = ''
        json_data = {'data': data, 'msg': res_msg, 'status': res_status, 'error': res_error}
    elif (status == 404):
        res_msg = "Service Not Found"
        res_status = 404
        res_error = ''
        json_data = {'data': data, 'msg': res_msg, 'status': res_status, 'error': res_error}
    return Response(json_data, status=status,content_type="application/json")
