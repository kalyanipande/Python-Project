import os

if os.getenv('KEYCLOAK_APP_NAME') is not None:
    KEY_CLOAK_APP = str(os.environ.get("KEYCLOAK_APP_NAME"))
else:
    KEY_CLOAK_APP = str(os.environ.get("KEYCLOAK_APP"))

if os.getenv('KEYCLOAK_HOST_NAME') is not None:
    KEYCLOAK_HOST = str(os.environ.get("KEYCLOAK_HOST_NAME"))
else:
    KEYCLOAK_HOST = str(os.environ.get("KEYCLOAK_HOST"))

KEY_CLOAK_URL = "https://" + KEYCLOAK_HOST + "/auth/"
KeycloakClienID = "admin-cli"
KEY_CLOAK_USERNAME = "superadmin"
KEY_CLOAK_PASSWORD = "admin"
KeycloakClienSecret = str(os.environ.get("KEYCLOAK_CLIENT_SECRET"))

ENABLE_PRINT = False

def cprint(msg=''):
    if ENABLE_PRINT:
        local_msg = msg
        if os.name == 'nt':  # Check Windows(nt)
            pass
        else:
            with open('/var/uam_logs.log', 'a') as f:
                print(local_msg, file=f)
