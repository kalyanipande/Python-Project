import os

os.system("python3 app/services/amq_subscriber.py &")
os.system("python3 manage.py makemigrations &")
os.system("python3 manage.py migrate &")
os.system("gunicorn route.wsgi:application --pythonpath webapp --workers=4 --threads=4  -k eventlet --bind 0.0.0.0:80 --timeout 150 --access-logfile - --error-logfile -")