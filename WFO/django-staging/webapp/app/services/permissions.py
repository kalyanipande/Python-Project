from rest_framework.permissions import BasePermission
from .commons import *
from rest_framework import exceptions

KEY_HEADER_TOKEN = "HTTP_TOKEN"
SWAGGER_API = {
    "/orch/api/":""
}
SKIP_TOKEN = {
    "/orch/api/ctoken":"",
    "/orch/api/logic/rule_status":"",
    "/orch/api/analysis/csvdownload":""
}

SPARK_DE_URLS = {
    "/orch/api/ctoken":"",
    "/orch/api/logic/bundle_info":"",
    "/orch/api/digimops-case-analysis/":"",
}
isTokenEnabled = True
# Custom permission for users with "is_active" = True.
class IsActive(BasePermission):
    """
    Allows access only to "is_active" users.
    """
    expects_authentication = False
    message = 'My Custom Message'

    def has_permission(self, request, view):
        # cprint("Permission Class iS_ACTIVE has_permission")
        # cprint(json.dumps(request.META))
        # raise exceptions.AuthenticationFailed('No such user')
        # return request.user and request.user.is_active
        # cprint("Before Reequest")
        req = request
        # cprint(req.method)
        if req.method != 'OPTIONS':
            # cprint("POST/GET Call")
            token = getRequestHeaders(req, KEY_HEADER_TOKEN)
            # cprint("Tokennnnnnnnnnnn - "+str(token))
            # cprint("dsafsdfd")
            route_path = str(req.path)

            # cprint('token = '+str(token))
            # cprint("route_path = "+route_path)
            challenges = ['Token type="Fernet"']
            # cprint(isTokenEnabled)
            if "/api/" in route_path:
                if checkKey(SWAGGER_API, route_path):
                    # cprint("checkKey - SWAGGER_API")
                    query = "select * from token where token='{}'".format(token)
                    # cprint(query)
                    res = connectFetchJSONWihtoutQueryDataNoResponse(query)
                    # cprint(res)
                    return True
                elif "/swagger" in route_path:
                    # cprint("swagger")
                    return True
                elif checkKey(SKIP_TOKEN, route_path):
                    # cprint("checkKey - SKIP_TOKEN")
                    return True
                elif isTokenEnabled == False:
                    cprint("isTokenEnabled")
                    return True
                else:
                    if token is None:
                        # cprint("token is None")
                        raise exceptions.AuthenticationFailed(
                            "Auth token required. Please provide an auth token as part of the request.")
                    elif not _token_is_valid(token):
                        # cprint("token - %s is invalid" % (token,))
                        raise exceptions.AuthenticationFailed(
                            "Auth token required. The provided auth token is not valid. Please request a new token and try again.")
                    query = "select * from token  where token='{}'".format(token)
                    # cprint(query)
                    res = connectFetchJSONWihtoutQueryDataNoResponse(query)
                    # print(")))))))))))))))))))))))",res)
                    if len(res):
                        if checkKey(SPARK_DE_URLS, route_path):
                            # cprint("True")
                            return True
                        else:
                            # cprint("False")
                            raise exceptions.AuthenticationFailed("Restricted")
                    return True
            else:
                return True

        else:
            # cprint("Options Call")
            return True


def _token_is_valid(token):
    if token_exists(token):  # Suuuuuure it's valid...
        query = "update token set updatedtime=%s where token=%s;"
        # cprint(query)
        query_data = ('now()', token)
        connectUpdationsNoResp(query, query_data)
        return True
    else:
        # logUserAuditRequestDetails(req,token,"Logged Out")
        return False
