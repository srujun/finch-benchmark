import argparse
import datetime as dt

from kafka import KafkaConsumer
from influxdb import InfluxDBClient


KAFA_SERVERS = ['ip-172-31-5-82:9092', 'ip-172-31-6-235:9092']
DB_NAME = 'latency-metrics'
SINK_TOPIC = 'sink1'


def main(args):
    consumer = KafkaConsumer(
        args.topic, value_deserializer=lambda m: m.decode('ascii'),
        bootstrap_servers=KAFA_SERVERS,
        # auto_offset_reset='earliest', enable_auto_commit=False
    )

    influx_ip, influx_port = args.influx.split(':')
    client = InfluxDBClient(host=influx_ip, port=influx_port, database=args.db)
    if args.db not in client.get_list_database():
        client.create_database(args.db)

    points = list()

    try:
        for msg in consumer:
            send_ts = msg.value.split(",")[0]
            recv_ts = dt.datetime.fromtimestamp(header['time']/1000).isoformat() + 'Z'

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


def get_args():
    parser = argparse.ArgumentParser(description='Process latency metrics')
    parser.add_argument('--influx', required=True,
                        help='influxdb ip:port')
    parser.add_argument('--db', default=DB_NAME,
                        help='influxdb database for latency metrics (default: %(default)s)')
    parser.add_argument('-t', '--topic', default=SINK_TOPIC,
                        help='kafka topic (default: %(default)s)')
    parser.add_argument('-k', '--kafka', nargs='*', default=KAFA_SERVERS,
                        help='kafka bootstrap servers (default: %(default)s)')

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main(get_args())
