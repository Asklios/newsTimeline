FROM python:3.9-alpine
RUN mkdir /app
WORKDIR /app
RUN mkdir data

RUN python -m venv /opt/venv
COPY requirements.txt .
RUN /opt/venv/bin/pip install -r requirements.txt

COPY main.py .
COPY sqlite_helper.py .

CMD ["/opt/venv/bin/python", "/app/main.py"]