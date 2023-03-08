# You can also use a unix socket for more efficiency
# gunicorn --workers=4 --threads=32 --bind unix:speechcatcher.sock --worker-class=gthread server:app

gunicorn --workers=4 --threads=32 --bind 127.0.0.1:6000 --worker-class=gthread server:app
