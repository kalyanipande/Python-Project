from django.utils.deprecation import MiddlewareMixin
from .utils import cprint


class ApplicationMiddlewares(MiddlewareMixin):
    def __init__(self,get_response):
        self.get_response = get_response
        
    def __call__(self,request):
        cprint("Application Authorization Values")
        return self.get_response(request)
