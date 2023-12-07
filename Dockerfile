# Dockerfile for Flask App
FROM python:3.9

WORKDIR /app

COPY . /app

# Install gunicorn
RUN pip install gunicorn

RUN pip install --no-cache-dir -r ./requirements.txt

RUN pip install pymysql mysqlclient

RUN pip install requests pycountry cloudscraper PyPDF2 retry rq


CMD ["sh", "-c", "python manage.py db init || python manage.py db migrate -m 'Initial migration' || python manage.py db upgrade || gunicorn -b 0.0.0.0:5500 -w 4 manage:app"]