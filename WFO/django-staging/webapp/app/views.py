from rest_framework.response import Response
from rest_framework.views import APIView

class TestAPI(APIView):
    def get(self,request,format=None):
        return Response({"data":"TestAPI"},status=200)