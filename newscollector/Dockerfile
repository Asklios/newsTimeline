FROM python:3.9-slim-buster
RUN mkdir /app
WORKDIR /app
RUN mkdir data

RUN python -m venv /opt/venv
COPY requirements.txt .
RUN /opt/venv/bin/pip install -r requirements.txt
RUN /opt/venv/bin/pip install -U pgcli --only-binary psycopg2

COPY . .

CMD ["/opt/venv/bin/python", "/app/main.py"]