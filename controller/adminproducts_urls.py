from django.urls import path, re_path
from . import productsController
from django.conf.urls import include, url

urlpatterns = [
    url(r'^products$', productsController.ProductCreate.as_view(), name='product_create_page'),
    url(r'^products/check_product$', productsController.ProductAvailability.as_view(), name='product_availability'),
]