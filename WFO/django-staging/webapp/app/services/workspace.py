from rest_framework.views import APIView
from django.views.decorators.csrf import csrf_exempt
from .sqldb_config import getRequestJSON ,response_wrapper,connectFetchfrompostgreaslist
import traceback
from route.settings import LOGGING
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")


class Taggenerator(APIView):
    # permission_classes = (IsActive,)
    @csrf_exempt
    def post(self,request):
        try:
            inputjson = getRequestJSON(request)
            query = ""
            if inputjson["type"] == "product":
                query = "select distinct productname from app_case_number_mapping where productname is NOT NULL"
            elif inputjson['type'] == "issuetype":
                query = "select distinct Issue_Type from tagdetails where product_name='" + inputjson['product'] + "'"
            elif inputjson['type'] == "issuedetail":
                query = "select distinct Issue_Details from tagdetails where product_name='" + inputjson['product'] + "' and Issue_Type='" + inputjson['issuetype'] + "'"
            elif inputjson['type'] == "additionaldetails":
                query = "select distinct Additional_Details from tagdetails where product_name='" + inputjson['product'] + "' and Issue_Type='" + inputjson['issuetype'] + "' and Issue_Details='" + inputjson['issuedetail'] + "'"
            elif inputjson['type'] == "releaseversion":
                query = "select distinct releaseversion from app_case_number_mapping where productname='" + inputjson['product'] + "'"
            else:
                logger.error("Invalid JSON ")
                resp = response_wrapper(500, "JSON input not valid")
                return resp
            resp=connectFetchfrompostgreaslist(query)
            logger.info("Data received from postgres")
        except Exception as e:
            logger.error(traceback.format_exc())
            resp=response_wrapper(500,str(e))
        return resp