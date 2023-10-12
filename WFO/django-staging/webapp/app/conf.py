import os

env = 'local'

if os.name != 'nt': #Check not Windows(nt)
    if os.getenv('DATA_CENTER') is not None:
        if os.getenv('KEYCLOAK_APP') is not None:
            if 'staging' in os.getenv('KEYCLOAK_APP'):
                env = os.getenv('DATA_CENTER')+'-dev'
            else:
                env = os.getenv('DATA_CENTER')
    if os.getenv('ENV') is not None:
        env = os.getenv('ENV')

ENVIRONMENT = {
    "env": env,
    "prod_env":["wfo-global-services-care-staging.asia","wfo-test-validation-staging.americas"],
    "app_trigger_inminutes":1,
    "termination_inminutes":30,
    "bundle_buffer_size_inmb":2048,
    "stomp_port":"61616",
    "asia-dev": {
        "webapp_name":"django",
        "api_ip":"localhost",
        "api_host": "0.0.0.0",
        "api_port":80,
        "api_debug": "True",
        "db_config": {
            "database": "postgres",
            "user": "adminalice",
            "password": "admin@123",
            # "host": "postgres",
            "host": "ai-postgres",
            "port": "5432",
            "host_external_ip": "production.aibi-prod-fp.ch-dc-os-gsn-107.k8s.dyn.nesc.nokia.net",
            # "host_external_port":"32552"
            "host_external_port":"31275"
        }
    },
    "digimops_config": {
        "user": "firappuser",
        "password": "resupparif@123",
        # "digimops_url": "https://gateway.ibus-fp.net/",
        "digimops_url": "https://gateway.ibus-fp.ext.net.nokia.com",
        "host_external_port":"31275",
        "base_url":"https://nokiawfoqa.ext.net.nokia.com/#/pages/table/case-recomendation/"
    },

}
