import datetime as dt
import json
from numbers import Number
from pprint import pprint

from kafka import KafkaConsumer
from influxdb import InfluxDBClient

INFLUX_IP = '192.168.1.4'
INFLUX_PORT = 8086

def main():
    consumer = KafkaConsumer(
        'metrics', value_deserializer=lambda m: json.loads(m.decode('ascii')),
        bootstrap_servers=['localhost:9092'],
        # auto_offset_reset='earliest', enable_auto_commit=False
    )
    client = InfluxDBClient(host=INFLUX_IP, port=INFLUX_PORT, database='streambench-metrics')

    for msg in consumer:
        points = list()

        header = msg.value['header']
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

if __name__ == '__main__':
    main()
