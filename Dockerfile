FROM python:3

ADD config.yaml /config/
ADD BloomskyToInflux.py /

RUN pip install bloomsky-API
RUN pip install PyYAML
RUN pip install influxdb

CMD [ "python", "./BloomskyToInflux.py" ]