from django.shortcuts import render
import json
from rest_framework.views import APIView
from rest_framework.response import Response
from django.views.generic import TemplateView
import time
from .utils import ENVIRONMENT_VARIABLE,cprint
import requests


class HealthCheck(TemplateView):
    template_name = 'healthcheck.html'

    def get(self,request,format=None):
        cprint("HealthCheckHealthCheckHealthCheckHealthCheck")
        env ={ "configs":
			[	
				{
					"title":"PostgreSQL",
					"subtitle":"Database",
					"img":"/orch/img/postgresql.png",
					"status":"Connected",
					"data":ENVIRONMENT_VARIABLE["db_config"]
				},
				{
					"title":"Redis",
					"subtitle":"Cache Management",
					"img":"/orch/img/redies.png",
					"status":"Connected",
					"data":ENVIRONMENT_VARIABLE["redis_config"]
				},
				{
					"title":"ActiveMQ Artemis",
					"subtitle":"Message Broker",
					"img":"/orch/img/activemq.png",
					"status":"Connected",
					"data":ENVIRONMENT_VARIABLE["bp_config"]
				},
				{
					"title":"Apache Airflow",
					"subtitle":"DAG",
					"img":"/orch/img/airflow.png",
					"status":"Connected",
					"data":ENVIRONMENT_VARIABLE["airflow_config"]
				},
				{
					"title":"Apache Camel",
					"subtitle":"Router",
					"img":"/orch/img/camel.png",
					"status":"Connected",
					"data":ENVIRONMENT_VARIABLE["camel_config"]
				},
				{
					"title":"Angular",
					"subtitle":"GUI",
					"img":"/orch/img/angular.png",
					"status":"Connected",
					"data":ENVIRONMENT_VARIABLE["angular_config"]
				}
			
		]
	}
        return render(request,self.template_name, env)

import psycopg2, redis
def postgres_test():
    # PostgreSQL DB Settings
    db=ENVIRONMENT_VARIABLE["db_config"]["database"]
    u=ENVIRONMENT_VARIABLE["db_config"]["user"]
    p=ENVIRONMENT_VARIABLE["db_config"]["password"]
    h=ENVIRONMENT_VARIABLE["db_config"]["host"] # Local
    prt=ENVIRONMENT_VARIABLE["db_config"]["port"]
    try:
        conn = psycopg2.connect("dbname='"+db+"' user='"+u+"' password='"+p+"' host='"+h+"' port='"+prt+"'")
        conn.close()
        return "Connected"
    except Exception as e:
        return "Not Connected"

import socket, stomp
def is_open(ip,port):
   s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   try:
      s.connect((ip, int(port)))
      s.shutdown(2)
      return True
   except Exception as e:
      return False

def redis_test():
    try:
        r = redis.StrictRedis(host=str(ENVIRONMENT_VARIABLE["redis_config"]["host"]),
							  port=str(ENVIRONMENT_VARIABLE["redis_config"]["port"]),
							  db=str(ENVIRONMENT_VARIABLE["redis_config"]["database"]),
							  password=str(ENVIRONMENT_VARIABLE["redis_config"]["password"]))
        r.ping()
        return "Connected"
    except Exception as e:
        return "Not Connected"

def activemq_test():
    try:
        c = stomp.Connection([(ENVIRONMENT_VARIABLE["bp_config"]["host"], ENVIRONMENT_VARIABLE["bp_config"]["port"])])
        c.connect(ENVIRONMENT_VARIABLE["bp_config"]["user"], ENVIRONMENT_VARIABLE["bp_config"]["password"])	
        return "Connected"
    except Exception as e:
        return "Not Connected"

def airflow_test():
	if is_open(str(ENVIRONMENT_VARIABLE["airflow_config"]["host"]),str(ENVIRONMENT_VARIABLE["airflow_config"]["port"])):
		return "Connected"
	else:
		return "Not Connected"

def camel_test():
	if is_open(str(ENVIRONMENT_VARIABLE["camel_config"]["host"]),str(ENVIRONMENT_VARIABLE["camel_config"]["port"])):
		return "Connected"
	else:
		return "Not Connected"

def angular_test():
	if is_open(str(ENVIRONMENT_VARIABLE["angular_config"]["host"]),str(ENVIRONMENT_VARIABLE["angular_config"]["port"])):
		return "Connected"
	else:
		return "Not Connected"

from psycopg2.extras import RealDictCursor
def get_global_status(size):
	db=ENVIRONMENT_VARIABLE["db_config"]["database"]
	u=ENVIRONMENT_VARIABLE["db_config"]["user"]
	p=ENVIRONMENT_VARIABLE["db_config"]["password"]
	h=ENVIRONMENT_VARIABLE["db_config"]["host"]  # Local
	prt=ENVIRONMENT_VARIABLE["db_config"]["port"]
	query = "select zip_id::text,status_msg,types,created_at::text from global_status limit "+str(size)
	conn = psycopg2.connect("dbname='"+db+"' user='"+u+"' password='"+p+"' host='"+h+"' port='"+str(prt)+"'")
	cur = conn.cursor(cursor_factory=RealDictCursor)
	cur.execute(query)
	return cur.fetchall()


class AccessDeniendPage(TemplateView):
    template_name = "common.html"

    def get(self,request,format=None):
        env ={ "configs": "AccessDeniendPage" }
        return render(request,self.template_name,env)   

class CookieExpiredPage(TemplateView):
    template_name = "common.html"

    def get(self,request,format=None):
        env ={ "configs": "CookieExpiredPage" }
        return render(request,self.template_name,env)

def produce_data_to_amqconsumer(json_data,topic_name):
	c = stomp.Connection([(ENVIRONMENT_VARIABLE["bp_config"]["host"], ENVIRONMENT_VARIABLE["bp_config"]["port"])])
	c.connect(ENVIRONMENT_VARIABLE["bp_config"]["user"], ENVIRONMENT_VARIABLE["bp_config"]["password"])
	c.send(body=json.dumps(json_data), destination='t_/'+str(topic_name))

class AMQLoadTesting(APIView):

    def get(self,request,format=None):
        for i in range(1,50):
            res_data = get_global_status(i)
            produce_data_to_amqconsumer(res_data,"mytest")
            time.sleep(2)
        return Response({"data":res_data},status=200)

class KeycloakUserLogout(APIView):

    def get(self,request,format=None):
        host = request.META["wsgi.url_scheme"]+"://"+request.META["HTTP_HOST"]
        url = host+"/gatekeeper/logout"
        headers = {'X-KC-Token': request.session['X-KC-Token']}
        r = requests.get(url,headers=headers, verify=False)
        if r.status_code == 200:
            return Response(json.loads(r.text),status=r.status_code)
        return Response({"data":"Server Error"},status=500)

    def post(self,request,format=None):
        pass