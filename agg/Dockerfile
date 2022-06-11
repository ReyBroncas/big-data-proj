FROM python:3.10-alpine

WORKDIR /opt/app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY main.py main.py

# Copy and enable your CRON task
COPY crontab /opt/app/crontab
RUN crontab /opt/app/crontab

RUN touch /tmp/out.log
CMD crond && tail -f /tmp/out.log

