import datetime as dt
import json
from numbers import Number
from pprint import pprint
import sys

from kafka import KafkaConsumer
from influxdb import InfluxDBClient

INFLUX_IP = 'localhost'
INFLUX_PORT = 8086
KAFA_SERVERS = ['localhost:9092']
DB_NAME = 'streambench-metrics'

def main():

    if len(sys.argv) > 1:
        INFLUX_IP = sys.argv[1]
        INFLUX_PORT = int(sys.argv[2])

    consumer = KafkaConsumer(
        'metrics', value_deserializer=lambda m: json.loads(m.decode('ascii')),
        bootstrap_servers=KAFA_SERVERS,
        # auto_offset_reset='earliest', enable_auto_commit=False
    )
    client = InfluxDBClient(host=INFLUX_IP, port=INFLUX_PORT, database=DB_NAME)
    if DB_NAME not in client.get_list_database():
        client.create_database(DB_NAME)

    try:
        for msg in consumer:
            points = list()

            header_hyphen = msg.value['header']
            header = {}
            for key, value in header_hyphen.items():
                header[key.replace('-', '')] = value
            metrics = msg.value['metrics']

            timestamp = dt.datetime.fromtimestamp(header['time']/1000).isoformat() + 'Z'
            header.pop('time')

            for class_, kv_pairs in metrics.items():
                point = {
                    'measurement': class_,
                    'tags': header,
                    'time': timestamp,
                    'fields': {}
                }

                for key, value in kv_pairs.items():
                    if isinstance(value, Number):
                        point['fields'][key] = value

                # pprint(point)
                if len(point['fields']) > 0:
                    points.append(point)

            client.write_points(points)
            print('Sent timestamp={}'.format(timestamp))
    except KeyboardInterrupt:
        print("Done!")

if __name__ == '__main__':
    main()
