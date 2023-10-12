import uuid
from rest_framework.views import APIView
from .commons import *
from django.views.decorators.csrf import csrf_exempt
import coreapi
from rest_framework import renderers, response, schemas
from rest_framework.schemas import ManualSchema

from django.conf import settings
# from route.settings import LOGGING
# import logging.config
# from ...route.settings import LOGGING
# logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")
import traceback
generator = schemas.SchemaGenerator(title='Bookings API')

# @auth_ns.route("/ctoken")
# @auth_ns.doc('Login User', body=req_login_user)
class LoginUser(APIView):
	schema = ManualSchema(fields=[
		coreapi.Field(
			"first_field",
			required=True,
			location="path",
		),
		coreapi.Field(
			"second_field",
			required=True,
			location="path",
		),
	])

	@csrf_exempt
	def post(self, request):
		"""
		Login User
		"""
		try:
			logger.info("Ctoken api")
			# req = request
			# breakpoint()
			# data = getRequestJSON(req)
			username = request.data['username']
			password = request.data['password']
			logger.info("Username:" + username)
			if username == "digimops":
				if user_exists(username,password):
					token = str(uuid.uuid4().hex.upper()[0:16])  # For Python2 working
					query = "insert into token (username,token,updatedtime,enabled) values(%s,%s,%s,%s);"
					query_data = (username, token, 'now()', '1')
					rows = connectUpdationsNormal(query, query_data)
					# query = "select authorities from authorities where username='" + uname + "'"
					# roles = connectNativeArray(query)
					json_data = {'data': {"token": token, "username": username},'error':'', 'msg': 'Token Created', 'message': "Token Created", 'status': 200}
					logger.info(json_data)
					resp = response_wrapper_plain_jsondumps(200, json_data)
					# logUserAuditRequestDetails(req, token=token, log_type="Logged In", username=uname)
				else:
					logger.info("User not exists")
					# logUserAuditRequestDetails(req, token=None, log_type="Failed to Logged In", username=uname)
					json_data = {"data": {"status": "Failed to login"},'error':'', 'msg': 'Failed to login', "message": "Failed to login", "title": "Error"}
					resp = response_wrapper(401, json_data)
				return resp
			isUserValidated = False
			if settings.env != "local":
				isUserValidated = user_exists_ldap(username,password)
			else:
				isUserValidated = user_exists(username,password)
			if isUserValidated:
				token = str(uuid.uuid4().hex.upper()[0:16])  # For Python2 working
				query = "insert into token (username,token,updatedtime,enabled) values(%s,%s,%s,%s);"
				query_data = (username, token, 'now()', '1')
				rows = connectUpdationsNormal(query, query_data)
				# query = "select authorities from authorities where username='" + uname + "'"
				# roles = connectNativeArray(query)
				# user_data = getUserDetailsByToken(token)
				# query = "select id as log_bundle_id,zip_name as log_bundle_name from log_bundle l where user_id=(select user_id from users where isdeleted = 0 and username='" + uname + "') and isdeleted=0"
				# user_log_bundle = connectNativeArrayAll(query)
				json_data = {'data': {"token": token, "username": username,},
							 'message': "Token Created",'error':'', "msg": "Token Created", 'status': 200}

				# logUserAuditRequestDetails(req, token=token, log_type="Logged In", username=uname)
				resp = response_wrapper_plain_jsondumps(200, json_data)
				return resp
			else:
				# logUserAuditRequestDetails(req, token=None, log_type="Failed to Logged In", username=uname)
				json_data = {"data": {"status": "Failed to login"}, "message": "Failed to login", "msg": "Failed to login", "title": "Error"}
				resp = response_wrapper(401, json_data)
				return resp
		except Exception as e:
			logger.error(str(e),traceback.format_exc())
			# logUserAuditRequestDetails(req, token=None, log_type="Failed to Logged In", username=data['uname'])
			json_data = {"data": {"status": "Failed to login"}, "message": "Failed to login", "msg": "Failed to login", "title": "Error"}
			resp = response_wrapper(500, json_data)
			return resp
