import os

ENVIRONMENT_VARIABLE = {
            "webapp_name":"wa-gui-care-alice-staging",
            "api_ip":"localhost",
            "api_host": "0.0.0.0",
            "api_port":8080,
            "api_debug": "True",
            "db_config": {
                "database": "adfhelm",
                "user": "admin",
                "password": "admin",
                "host": "pg-care-adfhelm-staging",
                "port": "5432",
            },
            "airflow_config": {
                "host": "af-care-adf-staging",
                "port": "8080",
            },
            "camel_config": {
                "host": "ca-care-adf-staging",
                "port": "8080",
            },
            "angular_config": {
                "host": "gui-staging",
                "port": "80",
            },
            "redis_config": {
                "database": "1",
                "cache_version":"1",
                "key_prefix":"wa-or/",
                "user": "",
                "password": "xDec3542",
                "host": "redis-care-adf-staging",
                "port": "6379",
                "host_external_ip": "10.135.177.25",
            },
            "bp_config": {
                "user": "artemis",
                "password": "simetraehcapa",
                "host": "as-care-americas-adfhelm-staging",
                "host_external_ip": "10.135.177.95",
                "port": "8080"
            },
            "isTokenEnabled": "True",
            "isLocalPrintEnabled": "True",
            "emil_ip": "10.133.131.202",
            "elk_config": {
                "ip": "es-care-alice-staging-es-http",
                "port": 9200,
                "raw_index": "spark_raw",
                "parser_index": "spark_parser",
                "rules_index": "spark_rules",
                "ml_index": "fn_output",
                "username": "elastic",
                "password": "lhkfnwj8brvtpc9flf2vgqrn"
            },
            "k8_s3_config": {
                "S3_BUCKET_NAME": "alice-care",
                "S3_BUCKET_ENV_NAME": "staging/data",
                "s3_key": "46849BVQEHLVWZY01NXT",
                "s3_secret": "JgMQZAds8iH4jcAcT84ilGHWqRPeT1dOtQSAyEwl",
                "s3_url": "http://minio.platform.svc.cluster.local:9000",
                "S3_UPLOAD_BUCKET_NAME": "alice-care-data"
            },
            "global_s3_config": {
                "S3_BUCKET_NAME": "alice-care",
                "S3_BUCKET_ENV_NAME": "staging/data",
                "s3_key": "46849BVQEHLVWZY01NXT",
                "s3_secret": "JgMQZAds8iH4jcAcT84ilGHWqRPeT1dOtQSAyEwl",
                "s3_url": "bh-dc-s3-dhn-15.eecloud.nsn-net.net",
                "S3_UPLOAD_BUCKET_NAME": "alice-input-logs"
            },
            "spark_config": {
                "region": "dev",
                "RM_IP": "rm-care-alice-staging",
                "RM_PORT": "5000",
                "QA_IP": "qa-care-alice-staging",
                "QA_PORT": "5001"
            },
            "rain_config": {
                "enabled": "True",
                "url": "https://rain.int.net.nokia.com/"
            }
        }

ENABLE_PRINT = True

def cprint(msg=''):
    if ENABLE_PRINT:
        local_msg = msg
        if os.name == 'nt':  # Check Windows(nt)
            pass
        else:
            with open('/var/proxy_logs.log', 'a') as f:
                print(local_msg, file=f)
