FROM python:3.10.5-slim-buster

COPY . usr/src/app
WORKDIR /usr/src/app

RUN pip install --upgrade pip
RUN pip install pandas
RUN pip install requests
RUN pip install sqlalchemy

ENTRYPOINT python main.py