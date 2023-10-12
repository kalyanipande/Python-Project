from django.utils.deprecation import MiddlewareMixin
from django.http import HttpResponseRedirect, HttpResponse
from .utils import cprint
import base64


class ProxyAuthTokenCheckMiddleware(MiddlewareMixin):

    def __init__(self,get_response):
        self.get_response = get_response
        
    def __call__(self,request):
        cprint("Request Processing")
        cprint("-------COOKIES Info---------")
        cprint(request.COOKIES)
        cprint("-------META Info---------")
        cprint(request.META)
        cprint("-------Headers Info---------")
        cprint(request.headers)
        if "X-Token-Status" in request.headers:
            if request.headers["X-Token-Status"] == "Token Invalid":
                return HttpResponse('Unauthorized', status=401)
        if "X-Proxy-Claims" in request.headers:
            cprint("X-Proxy-Claims precent in Headers")
            cprint("-------proxy_claims Info---------")
            proxy_claims = request.headers["X-Proxy-Claims"]
            proxy_claims = eval(base64.b64decode(proxy_claims))
            print("proxy_claims -----------",proxy_claims)
            request.session["X-KC-Token"]=request.headers["X-KC-Token"]
            request.session["preferred_username"]=proxy_claims["preferred_username"]
            request.session["roles"]=proxy_claims["realm_access"]["roles"]
            if "/orch/healthcheck/" in request.path :
                cprint("-------proxy_claims Info---------")
                proxy_claims = request.headers["X-Proxy-Claims"]
                proxy_claims = eval(base64.b64decode(proxy_claims))
                print("proxy_claims -----------",proxy_claims)
                groups = proxy_claims["groups"]
                if "/SUPPORT_TEAMS" not in groups:
                    resp= HttpResponseRedirect("/orch/access_denied/")
                    return resp
            elif "/orch/prometheus/metrics" in request.path :
                cprint(request.COOKIES)
                cprint("token precent in cookies")
                proxy_claims = request.headers["X-Proxy-Claims"]
                proxy_claims = eval(base64.b64decode(proxy_claims))
                print("proxy_claims -----------",proxy_claims)
                groups = proxy_claims["groups"]
                if "/SUPER_ADMINS" not in groups and "/admin" not in groups:
                    resp= HttpResponseRedirect("/orch/access_denied/")
                    return resp
        return self.get_response(request)