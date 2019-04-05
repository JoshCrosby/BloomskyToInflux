FROM python:3

ADD example_config.yaml /
ADD BloomskyToInflux.py /

RUN pip install bloomsky-API
RUN pip install PyYAML
RUN pip install influxdb

VOLUME /config

CMD [ "python", "./BloomskyToInflux.py" ]