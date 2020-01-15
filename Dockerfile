FROM python:3.6.9-stretch
RUN apt-get update && apt-get install cron -y

RUN mkdir -p /app
WORKDIR /app
COPY tap_zendesk /app/tap_zendesk
COPY tap_zendesk.egg-info /app/tap_zendesk.egg-info
COPY setup.py /app
RUN pip install -e '.' && pip install target-stitch && pip install python-crontab

COPY help-desk-catalog.json /app/
RUN touch /var/log/cron.log && chmod 0644 /var/log/cron.log
CMD ['tap-zendesk' '--start']
CMD cron && tail -f /var/log/cron.log