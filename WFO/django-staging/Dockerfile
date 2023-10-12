FROM abi-docker-local.artifactory-espoo1.int.net.nokia.com/abi/customers/django_optimised:1

COPY ./webapp /app
COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && curl https://packages.microsoft.com/config/debian/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list && apt-get update --allow-releaseinfo-change && ACCEPT_EULA=Y apt-get install -y msodbcsql18

RUN pip3 install -r requirements.txt

RUN apt install -y unixodbc-dev

CMD  python3 /app/run.py
