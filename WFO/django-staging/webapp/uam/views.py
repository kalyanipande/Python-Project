import time
import uuid

import pandas as pd
import requests
from rest_framework.decorators import api_view
from .models import Regions, User
from rest_framework.views import APIView
from rest_framework.response import Response
import traceback
import json
from . import settings
from route.settings import LOGGING
import logging.config

logging.config.dictConfig(LOGGING)
logger = logging.getLogger("WFOLogger")

toke_key = "access_token"


def get_token(request):
    url = settings.KEY_CLOAK_URL + "realms/" +  settings.KEY_CLOAK_APP + "/protocol/openid-connect/token"

    data = {"client_id": settings.KeycloakClienID, "client_secret": settings.KeycloakClienSecret,
            "username": settings.KEY_CLOAK_USERNAME, "password": settings.KEY_CLOAK_PASSWORD, "grant_type": "password"}
    r = requests.post(url, data=data)
    if r.status_code == 200:
        return json.loads(r.text)
    else:
        logger.error("The response is {}".format(r))
        return

# def get_token(request):
#     if "X-KC-Token" in request.session:
#         token_dict = {
#             "access_token":request.session["X-KC-Token"]
#         }
#         return token_dict
#     return

@api_view(['GET'])
def check_email_availability(request):
    regionlist = User.objects.filter(email=request.GET.get("email")).values('id', 'email')
    email = request.GET.get("email",'')
    key_cloak_user= getuser(request,email=email)
    logger.info(f"found user : {key_cloak_user}")
    if key_cloak_user:
        keys_present = key_cloak_user[0].keys()
        logger.info("inside username present {}".format(key_cloak_user))
        if "attributes" not in keys_present:
            logger.info("inside attributes")
            user_id = key_cloak_user[0]["id"]
            KEYCLOAK_USERS = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users"
            url = "{0}/{1}".format(KEYCLOAK_USERS, user_id)
            logger.info("url for delete : {}".format(url))
            token = get_token(request)
            logger.info("after token : {}".format(token))
            if token:
                access_token = token[toke_key]

                headers = {'Authorization': 'Bearer ' + access_token}
                response = requests.delete(
                    url, headers=headers, verify=False)
                logger.info("user deleted from keyclock{0}".format(response))
                key_cloak_user=[]
            else:
                raise Exception("access token exception")

    if regionlist or key_cloak_user:
        return Response({"data": {"availability": False}})
    else:
        return Response({"data": {"availability": True}})


@api_view(['GET'])
def check_user_availability(request):
    regionlist = User.objects.filter(username=request.GET.get("username")).values('id', 'username')
    username = request.GET.get("username",'')
    key_cloak_user = getuser(request,username=username)
    logger.info(f"user from keycloack : {key_cloak_user}")
    if key_cloak_user:
        keys_present = key_cloak_user[0].keys()
        logger.info("inside username present {}".format(key_cloak_user))
        if "attributes" not in keys_present:
            logger.info("inside attributes")
            user_id = key_cloak_user[0]["id"]
            KEYCLOAK_USERS = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users"
            url = "{0}/{1}".format(KEYCLOAK_USERS, user_id)
            logger.info("url for delete : {}".format(url))
            token = get_token(request)
            logger.info("after token : {}".format(token))
            if token:
                access_token = token[toke_key]

                headers = {'Authorization': 'Bearer ' + access_token}
                response = requests.delete(
                    url, headers=headers, verify=False)
                logger.info("user deleted from keyclock{0}".format(response))
                key_cloak_user = []
            else:
                raise Exception("access token exception")




    if regionlist or key_cloak_user:
        return Response({"data": {"availability": False}})
    else:
        return Response({"data": {"availability": True}})


@api_view(['GET'])
def get_region_list(request):
    regionlist = Regions.objects.values('id', 'regionname')
    return Response({"data": regionlist})


def getusers(request):
    token = get_token(request)
    access_token = ""
    if token:
        access_token = token[toke_key]
        url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users" + "?briefRepresentation=true&first=0&max=-1"
        headers = {'Authorization': 'Bearer ' + access_token}
        r = requests.get(url, headers=headers, verify=False)
        if r.status_code == 200:
            return json.loads(r.text)
        else:
            logger.error("The response is {}".format(r))
    return {}


def getuser(request,email=None,username=None):
    token = get_token(request)
    access_token = ""
    if token:
        access_token = token[toke_key]
        url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users"
        if email is not None:
            url = "{0}?email={1}".format(
                url, email)
        elif username is not None:
            url = "{0}?username={1}".format(
                url, username)
        else:
            raise Exception("Either email or username should be present to check availability")
        headers = {'Authorization': 'Bearer ' + access_token}
        logger.info("url for username :{}".format(url))
        r = requests.get(url, headers=headers, verify=False)
        if r.status_code == 200:
            return json.loads(r.text)
        else:
            logger.error("The response is {}".format(r))
    return {}


class KeyCloakUser(APIView):
    column_names = ['enabled', 'username', 'email', 'userRole', 'createdDate', 'updatedDate', 'updateComment',
                    'employee_name','team','bu_name','slm_email_id']

    def get(self, request, format=None):
        try:
            pageIndex = self.request.query_params.get('pageIndex', '')
            pageSize = self.request.query_params.get('pageSize', '')
            isFilter = self.request.query_params.get('isFilter')
            filterValues = {}
            column_names = ['enabled', 'username', 'email', 'userRole', 'createdDate', 'updatedDate',
                            'updateComment','employee_name','team','bu_name','slm_email_id']
            for i in range(len(column_names)):
                filterValues[column_names[i]] = self.request.query_params.get(column_names[i], '')
            if not filterValues['enabled'] == '':
                filterValues['enabled'] = True if filterValues['enabled'] == 'true' else False
            user = User.objects.values('id','username', 'email', 'userRole',
                                       'createdDate', 'updatedDate', 'updateComment',
                                       'employee_name','team','bu_name','slm_email_id')
            if not isFilter == 'False':
                if not filterValues['username'] == '':
                    user = user.filter(username__startswith=filterValues['username'])
                if not filterValues['email'] == '':
                    user = user.filter(email__startswith=filterValues['email'])
                # if not filterValues['region'] == '':
                #     user = user.filter(region__startswith=filterValues['region'])
                if not filterValues['createdDate'] == '':
                    user = user.filter(createdDate__startswith=filterValues['createdDate'])
                if not filterValues['updatedDate'] == '':
                    user = user.filter(updatedDate__startswith=filterValues['updatedDate'])
                if not filterValues['updateComment'] == '':
                    user = user.filter(updateComment=filterValues['updateComment'])
                # if not filterValues['enabled'] == '':
                #     user = user.filter(enabled=filterValues['enabled'])
                if not filterValues['userRole'] == '':
                    user = user.filter(userRole=filterValues['userRole'])

                if not filterValues['employee_name'] == '':
                    user = user.filter(userRole=filterValues['employee_name'])
                if not filterValues['team'] == '':
                    user = user.filter(userRole=filterValues['team'])
                if not filterValues['bu_name'] == '':
                    user = user.filter(userRole=filterValues['bu_name'])
                if not filterValues['slm_email_id'] == '':
                    user = user.filter(userRole=filterValues['slm_email_id'])
            user = user.order_by('-updatedDate', '-createdDate')
            logger.info("The use is {}".format(user))
            df = pd.DataFrame(user)
            totalRecords = len(df)
            logger.info("The total records count is {}".format(totalRecords))
            df.reset_index()
            df = df.iloc[int(pageIndex):int(pageSize)]
            df = df.fillna('')
            finalResult = {"data": df.to_dict(orient='records'), "totalRecords": totalRecords}
            return Response(finalResult, status=200)
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)

    def post(self, request, format=None):
        req_data = request.data
        try:
            token = get_token(request)
            access_token = ""
            if token:
                access_token = token[toke_key]
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users"+"?briefRepresentation=true&first=0&max=-1"
                headers = {'Authorization': 'Bearer '+access_token,'Content-type': 'application/json'}
                data = {"username": req_data['username'],"enabled":True,
                        "attributes": {"userRole": req_data["userRole"]["id"],
                                       "createdDate": req_data['createdDate'], "updatedDate": req_data['updatedDate']
                                       }}
                logger.info("The data is {}".format(data))
                user = User()
                user.username = req_data['username']
                if "email" in req_data:
                    data["email"] = req_data["email"]
                    user.email = req_data["email"]
                # if "firstName" in req_data:
                #     data["firstName"] = req_data["firstName"]
                #     user.firstName = req_data["firstName"]
                # if "lastName" in req_data:
                #     data["lastName"] = req_data["lastName"]
                #     user.lastName = req_data["lastName"]
                # if "enabled" in req_data:
                #     user.enabled = True if req_data["enabled"] == 'true' else False
                # if "region" in req_data:
                #     user.region = ','.join(req_data['region']).replace(" ", "")
                if "createdDate" in req_data:
                    user.createdDate = req_data["createdDate"]
                if "updatedDate" in req_data:
                    user.updatedDate = req_data["updatedDate"]
                if "userRole" in req_data:
                    user.userRole = req_data["userRole"]["id"]

                if "employee_name" in req_data:
                    user.employee_name = req_data["employee_name"]
                if "team" in req_data:
                    user.team = req_data["team"]
                if "bu_name" in req_data:
                    user.bu_name = req_data["bu_name"]
                if "slm_email_id" in req_data:
                    user.slm_email_id = req_data["slm_email_id"]

                logger.info(f"The data is data is {data} and the url is {url}")
                getHeaders = {'Authorization': 'Bearer ' + access_token}
                userList = requests.get(url, headers=getHeaders, verify=False)
                if userList.status_code == 200:
                    tmp = json.loads(userList.text)
                    for i in range(len(tmp)):
                        if "attributes" in tmp[i]:
                            tmp2 = tmp[i]["attributes"]
                            tmp[i].pop('attributes', None)
                            for key in tmp2:
                                if type(tmp2[key]) is list:
                                    tmp2[key] = ', '.join(tmp2[key])

                            tmp[i].update(tmp2)
                    df = pd.DataFrame(tmp)
                    df1 = df[['id','enabled', 'username', 'email']]
                    df2 = df1.loc[lambda df1: df1['email'] == data['email']].to_dict(orient='records')
                if len(df2) == 0:
                    r = requests.post(url, data=json.dumps(data), headers=headers, verify=False)
                    print(">>>>>>>>>>>>>><<<<<<<<<<<<<<<<<",r.status_code)
                    if r.status_code == 200 or r.status_code == 201:
                        # time.sleep(2)
                        users = getusers(request)
                        filter_user = list(filter(lambda x: x["username"] == req_data['username'], users))
                        print(filter_user)
                        if len(filter_user) > 0:
                            user.id = filter_user[0]["id"]
                        user.save()
                        # user role saving
                        index = user.id
                        groupid = req_data["userRole"]["id"]
                        print("================group id is",groupid)
                        url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users" + "/" + str(index) + "/groups/" + groupid
                        logger.info("------------- ===============")
                        logger.info(url)
                        headers = {'Authorization': 'Bearer ' + access_token}
                        resRole = requests.put(url, headers=headers, verify=False)
                        logger.info(resRole.status_code)
                        logger.info(resRole.text)
                        logger.info("===================== End...")
                        newUser = User.objects.values('id','username', 'email', 'userRole','createdDate', 'updatedDate', 'updateComment','employee_name','team','bu_name','slm_email_id')
                        ldf = pd.DataFrame(newUser)
                        totalRecords = len(ldf)
                        return Response(
                            {"title": "new user", "message": "user created successfully", "id": filter_user[0]["id"],"totalRecords": totalRecords}, status=201)
                else:
                    index = df2[0]["id"]
                    url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users/" + index
                    headers = {'Authorization': 'Bearer ' + access_token, 'Content-type': 'application/json'}
                    r = requests.put(url, data=json.dumps(data), headers=headers, verify=False)
                    if r.status_code == 200 or r.status_code == 201 or r.status_code == 204:
                        user.id = index
                        user.save()
                        userid = index
                        if "olduserRole" in req_data:
                            groupid = req_data["olduserRole"]
                            url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users" + "/" + str(userid) + "/groups/" + groupid
                            logger.info("------------- ===============")
                            logger.info(url)
                            headers = {'Authorization': 'Bearer ' + access_token}
                            r = requests.delete(url, headers=headers, verify=False)
                            logger.info(r.status_code)
                            logger.info(r.text)
                            logger.info("===================== End...")
                        if "userRole" in req_data:
                            groupid = req_data["userRole"]["id"]
                            url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users" + "/" + str(userid) + "/groups/" + groupid
                            logger.info("------------- ===============")
                            logger.info(url)
                            headers = {'Authorization': 'Bearer ' + access_token}
                            r = requests.put(url, headers=headers, verify=False)
                            logger.info(r.status_code)
                            logger.info(r.text)
                            logger.info("===================== End...")
                        newUser = User.objects.values('id','username', 'email', 'userRole','createdDate', 'updatedDate', 'updateComment','employee_name','team','bu_name','slm_email_id')
                        ldf = pd.DataFrame(newUser)
                        totalRecords = len(ldf)
                        return Response({"title": "new user", "message": "user updated successfully", "id": index,"totalRecords": totalRecords}, status=201)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)


class KeyCloakUserDetail(APIView):

    def get(self, request, id, format=None):
        try:
            token = get_token(request)
            access_token = ""
            if token:
                access_token = token[toke_key]
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users/" + id
                headers = {'Authorization': 'Bearer ' + access_token}
                r = requests.get(url, headers=headers, verify=False)
                if r.status_code == 200:
                    return Response(json.loads(r.text), status=r.status_code)
                else:
                    logger.error("The failed response is", r.text)
                    return Response({"data": "Request Failed"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)

    def post(self, request, id, format=None):
        pass

    def put(self, request, id, format=None):

        req_data = request.data
        try:
            token = get_token(request)
            access_token = ""
            if token:
                access_token = token[toke_key]
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users/" + id
                headers = {'Authorization': 'Bearer ' + access_token, 'Content-type': 'application/json'}
                user = User.objects.filter(id=id)
                if user:
                    user = User.objects.get(id=id)
                else:
                    user = User()
                    user.id = id
                data = {"username": req_data['username'],
                        "attributes": {"userRole": req_data["userRole"],
                                       "createdDate": req_data['createdDate'], "updatedDate": req_data['updatedDate']}}
                user.username = req_data['username']
                if "email" in req_data:
                    data["email"] = req_data["email"]
                    user.email = req_data["email"]
                # if "firstName" in req_data:
                #     data["firstName"] = req_data["firstName"]
                #     user.firstName = req_data["firstName"]
                # if "lastName" in req_data:
                #     data["lastName"] = req_data["lastName"]
                #     user.lastName = req_data["lastName"]
                # if "enabled" in req_data:
                #     user.enabled = True if req_data["enabled"] == 'true' else False
                # if "region" in req_data:
                #     user.region = ','.join(req_data['region']).replace(" ", "")
                if "createdDate" in req_data:
                    user.createdDate = req_data["createdDate"]
                if "updatedDate" in req_data:
                    user.updatedDate = req_data["updatedDate"]
                if "userRole" in req_data:
                    user.userRole = req_data["userRole"]["id"]

                if "employee_name" in req_data:
                    user.employee_name = req_data["employee_name"]
                if "team" in req_data:
                    user.team = req_data["team"]
                if "bu_name" in req_data:
                    user.bu_name = req_data["bu_name"]
                if "slm_email_id" in req_data:
                    user.slm_email_id = req_data["slm_email_id"]

                r = requests.put(url, data=json.dumps(data), headers=headers, verify=False)
                if r.status_code == 200 or r.status_code == 201 or r.status_code == 204:
                    user.save()
                    userid = user.id
                    if "olduserRole" in req_data:
                        groupid = req_data["olduserRole"]
                        url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users" + "/" + str(userid) + "/groups/" + groupid
                        logger.info("------------- ===============")
                        logger.info(url)
                        headers = {'Authorization': 'Bearer ' + access_token}
                        r = requests.delete(url, headers=headers, verify=False)
                        logger.info(r.status_code)
                        logger.info(r.text)
                        logger.info("===================== End...")
                    if "userRole" in req_data:
                        # groupid = req_data["userRole"]
                        groupid = req_data["userRole"]["id"]
                        url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users" + "/" + str(userid) + "/groups/" + groupid
                        logger.info("------------- ===============")
                        logger.info(url)
                        headers = {'Authorization': 'Bearer ' + access_token}
                        r = requests.put(url, headers=headers, verify=False)
                        logger.info(r.status_code)
                        logger.info(r.text)
                        logger.info("===================== End...")
                    return Response({"message": "user edit successfully"}, status=200)
                else:
                    logger.error("The failed response is ", r.text)
                    return Response({"message": "user edit not saved"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)

    def delete(self, request, id, format=None):
        req_data = request.data
        try:
            token = get_token(request)
            access_token = ""
            if token:
                access_token = token[toke_key]
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users/" + id
                headers = {'Authorization': 'Bearer ' + access_token}
                r = requests.delete(url, headers=headers, verify=False)
                if r.status_code == 200 or r.status_code == 201 or r.status_code == 204:
                    user = User.objects.filter(id=id).delete()
                    return Response({"message": "user delete successfully"}, status=204)
                else:
                    return Response({"message": "user delete not saved"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)


class KeyCloakGroup(APIView):

    def get(self, request, format=None):
        try:
            token = get_token(request)
            access_token = ""
            if token:
                access_token = token[toke_key]
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/groups"
                headers = {'Authorization': 'Bearer ' + access_token}
                r = requests.get(url, headers=headers, verify=False)
                if r.status_code == 200:
                    return Response(json.loads(r.text), status=r.status_code)
                return Response({"data": "Request Failed"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)


class KeycloakGroupMap(APIView):

    def get(self, request, format=None):
        try:
            token = get_token(request)
            access_token = ""
            data = request.GET
            if token:
                userid = data.get("userid")
                logger.info("The user id is {}".format(userid))
                access_token = token[toke_key]
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users" + "/" + userid + "/groups"
                headers = {'Authorization': 'Bearer ' + access_token}
                r = requests.get(url, headers=headers, verify=False)
                logger.info("===== ....... In view ..... ======")
                logger.info(r.text)
                logger.info(r.status_code)
                if r.status_code == 200:
                    return Response(json.loads(r.text), status=r.status_code)
                else:
                    logger.error("The failed response is {}".format(r))
                    return Response({"data": "Request Failed"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)

    def post(self, request, format=None):

        req_data = request.data
        try:
            token = get_token(request)
            access_token = ""
            if token:
                access_token = token[toke_key]
                userid = req_data["userid"]
                groupid = req_data["groupid"]
                logger.info("The userid {} and group id is {}".format(userid, groupid))
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users" + "/" + userid + "/groups/" + groupid
                logger.info("------------- ===============")
                logger.info(url)
                headers = {'Authorization': 'Bearer ' + access_token}
                r = requests.put(url, headers=headers, verify=False)
                logger.info(r.status_code)
                logger.info(r.text)
                logger.info("===================== End...")
                if r.status_code == 200 or r.status_code == 204:
                    return Response({"message": "user added with group"}, status=r.status_code)
                return Response({"data": "res_data"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)

    def delete(self, request, format=None):

        req_data = request.data
        try:
            token = get_token(request)
            access_token = ""
            if token:
                access_token = token[toke_key]
                userid = req_data["userid"]
                groupid = req_data["groupid"]
                logger.info("The userid {} and group id is {}".format(userid, groupid))
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users" + "/" + userid + "/groups/" + groupid
                logger.info("------------- ===============")
                logger.info(url)
                headers = {'Authorization': 'Bearer ' + access_token}
                r = requests.delete(url, headers=headers, verify=False)
                logger.info(r.status_code)
                logger.info(r.text)
                logger.info("===================== End...")
                if r.status_code == 200 or r.status_code == 204:
                    return Response({"message": "user removed with group"}, status=r.status_code)
                return Response({"data": "Request Failed"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)


class KeyCloakGroup(APIView):

    def get(self, request, format=None):
        try:
            token = get_token(request)
            access_token = ""
            if token:
                access_token = token[toke_key]
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/groups"
                headers = {'Authorization': 'Bearer ' + access_token}
                r = requests.get(url, headers=headers, verify=False)
                if r.status_code == 200:
                    df = pd.DataFrame(eval(r.text))
                    df.set_index("name", inplace=True)
                    df = df.loc[["ROLE_ADMIN", "ROLE_USER"]]
                    df = df.rename_axis()
                    df['name'] = ["ROLE_ADMIN", "ROLE_USER"]
                    df[['id', 'name', 'path', 'subGroups']]
                    return Response(df.to_dict(orient="records"), status=r.status_code)
                else:
                    logger.error("The response is {}".format(r))
                    return Response({"data": "Request Failed"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)


class KeycloakGroupMapDelete(APIView):

    def get(self, request, format=None):
        pass

    def post(self, request, format=None):

        req_data = request.data
        try:
            token = get_token(request)
            access_token = ""
            if token:
                access_token = token[toke_key]
                userid = req_data["userid"]
                groupid = req_data["groupid"]
                logger.info("The userid {} and group id is {}".format(userid, groupid))
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users" + "/" + userid + "/groups/" + groupid
                logger.info("------------- ===============")
                logger.info(url)
                headers = {'Authorization': 'Bearer ' + access_token}
                r = requests.delete(url, headers=headers, verify=False)
                logger.info(r.status_code)
                logger.info(r.text)
                logger.info("===================== End...")
                if r.status_code == 200 or r.status_code == 204:
                    return Response({"message": "user removed with group"}, status=r.status_code)
                return Response({"data": "Request Failed"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)


class GetEvents(APIView):

    def get(self, request, format=None):
        try:
            token = get_token(request)
            access_token = ""
            data = request.GET
            pageIndex = self.request.query_params.get('pageIndex', '')
            pageSize = self.request.query_params.get('pageSize', '')
            timeValue = self.request.query_params.get('time', '')
            userNameValue = self.request.query_params.get('username', '')
            typeValue = self.request.query_params.get('type', '')
            ipAddressValue = self.request.query_params.get('ipAddress', '')
            isFilter = self.request.query_params.get('isFilter', '')
            if token:
                userid = data.get("userid")
                logger.info("The userid is {}".format(userid))
                access_token = token[toke_key]
                url = settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/events"
                headers = {'Authorization': 'Bearer ' + access_token}
                r = requests.get(url, headers=headers, verify=False)
                logger.info("===== ....... In view ..... ======")
                logger.info(r.text)
                logger.info(r.status_code)
                if r.status_code == 200:
                    r_text = json.loads(r.text)
                    df = pd.DataFrame(eval(r.text))
                    if not len(df) == 0:
                        df1 = df[['time', 'type', 'ipAddress']]
                        nameDf = df['details'].values.tolist()
                        namedf = pd.DataFrame(nameDf, columns=['username'])
                        df1 = pd.concat([df1, namedf], axis=1)
                        df1['time'] = pd.to_datetime(df1['time'], unit='ms')
                        df1['time'] = df1['time'].dt.strftime('%d/%m/%Y %H:%M %p')
                        df1 = df1.astype(str).apply(lambda x: x.str.lower())
                        if not isFilter == 'False':
                            df2 = df1.loc[(df1['username'].str.contains(userNameValue, case=False)) & (
                                df1['type'].str.contains(typeValue, case=False)) & (
                                              df1['ipAddress'].str.contains(ipAddressValue, case=False)) & df1[
                                              'time'].str.contains(timeValue, case=False)].copy()
                            totalRecords = len(df2)
                            logger.info("The total records count is {}".format(totalRecords))
                            df2.reset_index(inplace=True)
                            df2 = df2.iloc[int(pageIndex):int(pageSize)]
                            df2 = df2.fillna('')
                            res = df2.to_dict(orient='records')
                            finalResult = {"data": res, "totalRecords": totalRecords}
                        else:
                            totalRecords = len(df1)
                            logger.info("The total records count is {}".format(totalRecords))
                            df1 = df1.reset_index()
                            df1 = df1.iloc[int(pageIndex):int(pageSize)]
                            df1 = df1.fillna('')
                            res = df1.to_dict(orient="records")
                            finalResult = {"data": res, "totalRecords": totalRecords}
                        return Response(finalResult, status=r.status_code)
                    else:
                        res = {"data": [], "totalRecords": 0}
                        return Response(res, status=200)
                return Response({"data": "Request Failed"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)


class KeycloakUserLogout(APIView):

    def get(self, request, format=None):
        try:
            token = get_token(request)
            access_token = ""
            if token:
                host = request.META["wsgi.url_scheme"] + "://" + request.META["HTTP_HOST"]
                url = host + "/gatekeeper/logout"
                logger.info(url)
                headers = {'X-KC-Token': request.session['X-KC-Token']}
                r = requests.get(url, headers=headers, verify=False)
                if r.status_code == 200:
                    return Response(json.loads(r.text), status=r.status_code)
                else:
                    logger.error("The response is {}".format(r))
                    return Response({"data": "Request Failed"}, status=r.status_code)
            else:
                logger.error("The access token is {}".format(token) + " and Authentication got Failed")
        except Exception as e:
            logger.error(str(e), traceback.format_exc())
            return Response({"data": "Internal server error"}, status=500)

    def post(self, request, format=None):
        pass



class UnAuthecateUser(APIView):
    def get(self, request):
        try:
            logger.info("inside unauth : {}".format(request))
            token = get_token(request)
            logger.info("after token : {}".format(token))
            if token:
                access_token = token[toke_key]
                url  = settings.KEY_CLOAK_URL + "admin/realms/" +  settings.KEY_CLOAK_APP + "/users"
                url = "{0}?username={1}".format(
                    url, self.request.META['QUERY_STRING'])
                logger.info("url for query : {}".format(url))
                headers = {'Authorization': 'Bearer ' + access_token}
                response = requests.get(url, headers=headers,
                                        verify=False)
                res_obj = json.loads(response.text)
                logger.info("Response1 {0}".format(res_obj))
                if 'error' in res_obj:
                    return Response({"data": res_obj}, status=500)
                if response.status_code == 200:
                    user_id = res_obj[0]["id"]
                    keys_present = res_obj[0].keys()
                    if "attributes" not in keys_present:
                        KEYCLOAK_USERS=settings.KEY_CLOAK_URL + "admin/realms/" + settings.KEY_CLOAK_APP + "/users"
                        url = "{0}/{1}".format(KEYCLOAK_USERS, user_id)
                        logger.info("url for delete : {}".format(url))
                        headers = {'Authorization': 'Bearer ' + access_token}
                        response = requests.delete(
                            url, headers=headers, verify=False)
                        logger.info("user deleted from keyclock{0}".format(response))
                        return Response(res_obj, status=response.status_code)
                    else:
                        return Response({"data": {"status": "Success", "message": "USER CANNOT BE DELETED"}}, status=200)
                return Response({"data": {"status": "Error", "message": "NO_RECORD_FOUND"}},
                                    status="HTTP_API_ERROR")
        except Exception as e:
            logger.error(traceback.format_exc(), 'error')
            return Response({"data": {"status": "Error", "message": e}}, status=500)