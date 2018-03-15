import json
import pickle
import socket
import struct

from pprint import pprint

from kafka import KafkaConsumer

CARBON_IP = 'localhost'
CARBON_PORT = 9109

def main():
    consumer = KafkaConsumer(
        'metrics', value_deserializer=lambda m: json.loads(m.decode('ascii')),
        bootstrap_servers=['localhost:9092'],
        # auto_offset_reset='earliest', enable_auto_commit=False
    )
    carbon = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    carbon.connect((CARBON_IP, CARBON_PORT))

    for msg in consumer:
        # metric_tuples = list()

        header = msg.value['header']
        metrics = msg.value['metrics']

        timestamp = header['time']
        job_name = header['job-name']

        for class_, kv_pairs in metrics.items():
            for key, value in kv_pairs.items():
                path = '{}:{}:{}'.format(job_name, class_, key)
                tuple_ = (path, (timestamp, value))
                # metric_tuples.append(tuple_)
                carbon_message = '{} {} {}'.format(path, value, timestamp)
                carbon.sendall(carbon_message.encode())

        # pprint(metric_tuples)

        # payload = pickle.dumps(metric_tuples, protocol=2)
        # payload_header = struct.pack("!L", len(payload))
        # carbon_message = payload_header + payload

        # carbon.sendall(carbon_message)
        print('Sent timestamp={}'.format(timestamp))

if __name__ == '__main__':
    main()
