#!/bin/sh

# Bloomsky API Notes:
#  - The Sky2 generally seems to update data every 5 minutes, per its timestamp
#  - The Storm updates data far more requently, and doesn't provide a timestamp

# This is only tested on my own instance which has a SKY2 & Storm devices


import bloomsky_api, json, yaml, datetime, pytz, re, logging, time, sys
from influxdb import InfluxDBClient

try:
    with open("/config/config.yaml", "r") as ymlfile:
        CONFIG = yaml.load(ymlfile)
except FileNotFoundError:
    from shutil import copyfile
    copyfile("/example_config.yaml","/config/config.yaml")
    print('Please edit config.yaml and enter your API and InfluxDB settings')
    exit(1)


def main():
    influx_client = InfluxDBClient(host=CONFIG['influx']['host'], port=CONFIG['influx']['port'])

    try:
        dbs = influx_client.get_list_database()
    except Exception as e:
        logging.error('Error connecting to InfluxDB - is influx running?: ', e)
        return

    # Create DB if not in Influx
    if list(filter(lambda database: database['name'] == CONFIG['influx']['database'], dbs)) == []:
        logging.info("Creating database: " + CONFIG['influx']['database'])
        influx_client.create_database(CONFIG['influx']['database'])

    influx_client.switch_database(CONFIG['influx']['database'])

    data = getBloomskyData()

    if data == '':
        logging.error("%s API data response empty", datetime.datetime.now())
        return

    logging.debug(data)

    # iterate returned Bloomsky devices
    for device in data:
        influx_client.write_points(jsonTranspose(device))
        logging.info("Processed for device: %s", device['DeviceName'])


def getBloomskyData():
    bloomsky_client = bloomsky_api.BloomSkyAPIClient(api_key=CONFIG['bloomsky']['apikey'])
    return bloomsky_client.request_data(True)._json


def convertTime(timestamp):
    return grafanaFriendlyTime(datetime.datetime.fromtimestamp(timestamp))


def grafanaFriendlyTime(timestamp):
    return timestamp.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def convertVideoList(videos, tags):
    metrics = []
    for v in videos:
        # Regex the date
        strDate = re.search("([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))",v)
        if strDate == None:
            logging.warning('Could not get video date from URL: ' + v)
            continue
        dateObj = datetime.datetime.strptime(strDate.group(), "%Y-%m-%d")
        metrics += [createInfluxMetric('timelapse', tags, grafanaFriendlyTime(dateObj), {"timelapse_url": v})]
    return metrics


def createInfluxMetric(measurement, tags, time, fields):
    jsonObject = {
        "measurement": measurement,
        "tags": tags,
        "time": time,
        "fields": fields
    }
    return jsonObject


# Logic to tanspose Bloomsky returned JSON to InfluxDB JSON measurements
def jsonTranspose(bloomskyDevice):
    dataPoints = []

    # Get tags from json as defined in config
    tags = {}
    for tag in CONFIG['bloomsky']['tag_names']:
        if tag in bloomskyDevice:
            tags[tag] = bloomskyDevice[tag]

    # Camera measurement
    if ('Data' in bloomskyDevice and 'ImageURL' in bloomskyDevice['Data'] and 'ImageTS' in bloomskyDevice['Data']):
        dataPoints += [createInfluxMetric('camera', tags, convertTime(bloomskyDevice['Data']['ImageTS']), \
                                          {"image_url": bloomskyDevice['Data']['ImageURL']})]
        # we can remove this object so it's not added to the bloomskyDevice measurement below
        del bloomskyDevice['Data']['ImageTS']
        del bloomskyDevice['Data']['ImageURL']
    else:
        logging.warning('API has no camera data')

    # Timelapse measurement
    if (CONFIG['bloomsky']['celsius'] and 'VideoList_C' in bloomskyDevice):
        dataPoints += convertVideoList(bloomskyDevice['VideoList_C'], tags)
    elif not CONFIG['bloomsky']['celsius'] and 'VideoList' in bloomskyDevice:
        dataPoints += convertVideoList(bloomskyDevice['VideoList'], tags)
    else:
        logging.warning('No timelapse URL data in API object')

    # bloomskyDevice measurement
    if 'Data' in bloomskyDevice and 'DeviceType' in bloomskyDevice['Data']:
        # remove 'TS' from Data
        bloomskyDeviceTime = bloomskyDevice['Data']['TS']
        del bloomskyDevice['Data']['TS']
        dataPoints += [createInfluxMetric(bloomskyDevice['Data']['DeviceType'], tags, \
                                          convertTime(bloomskyDeviceTime), bloomskyDevice['Data'])]
    else:
        logging.error("API didn't return 'Data' or 'DeviceType' objects, no data")

    # Storm measurement
    if ('Storm' in bloomskyDevice and bloomskyDevice['Storm'] != {}):
        dataPoints += [createInfluxMetric('Storm', tags, grafanaFriendlyTime(datetime.datetime.now()), \
                                          bloomskyDevice['Storm'])]

    return dataPoints


# Call main()
if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, \
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(funcName)s - line %(lineno)d")
    while True:
        try:
            main()
            time.sleep(CONFIG['interval'])
        except KeyboardInterrupt as ex:
            logging.info('Exiting!')
            break
