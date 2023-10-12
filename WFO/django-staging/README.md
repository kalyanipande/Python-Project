## **Webapp - Django - Orchestrator**

Django is an open-source python web framework used for rapid development, pragmatic, maintainable, clean design, and secure websites. A web application framework is a toolkit of all components needed for application development.

## **Deployment**

##### **Ingress Configuration for Gatekeeper**
    - nginx.ingress.kubernetes.io/auth-url - Gatekeeper auth URL
    - nginx.ingress.kubernetes.io/auth-signin - Gatekeeper Signin URL
    - nginx.ingress.kubernetes.io/configuration-snippet - Set headers & Cookies for requested host
    - Path [/] route to Angular GUI service
    - Path [/orch/] route to Django API service
    - Path [/v1/] route to Hasura GraphQL service

  Refer - [Ingress Oauth External Authentication](https://kubernetes.github.io/ingress-nginx/examples/auth/oauth-external-auth/)
  Refer - [Ingress Configuration Snippet](https://kubernetes.github.io/ingress-nginx/examples/customization/configuration-snippets/)
  Refer - [Ingress Multiple Path for Host](https://kubernetes.io/docs/concepts/services-networking/ingress/)

##### **Component Specification values are configured in deployment.yaml, if necessary change the values based on the requriment**

Refer - [CPU & Memory Values](https://confluence.int.net.nokia.com/display/ABI/Component+CPU+Memory)

##### **CI/CD environment variables**

Refer - [CI/CD Variable Value](https://confluence.int.net.nokia.com/display/ABI/CI+CD+Environment)
    
    - CI_APP_NAME*
    - CI_BUSINESS
    - CI_DATA_CENTER
    - CI_DECRYPTOR_IMAGE_TAG*
    - CI_DNS
    - CI_NAMESPACE
    - CI_KEYCLOAK_HOST
    - CI_PLATFORM
    - CI_PROJECT
    - CI_IS_SSO_ENABLED
    - CI_REGISTRY*
    - CI_REGISTRY_TOKEN*
    - CI_REGISTRY_USER*

    All variables values must be present before running deployment pipeline
    * Represent the values are fixed (Restriction for change the value)
    

## **Project Setup**

Refer - [Project Setup](https://confluence.int.net.nokia.com/display/ABI/Component+Deployment)

## **Reference Links**

- [Django Documentation](https://docs.djangoproject.com/en/stable/)
- [Django REST Documentation](https://www.django-rest-framework.org/)
- [Django Setup](https://django-extensions.readthedocs.io)
- [Ingress Auth Configs](https://kubernetes.github.io/ingress-nginx/examples/auth/oauth-external-auth/)