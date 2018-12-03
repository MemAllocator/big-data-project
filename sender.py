#!/usr/bin/env python
import pika
import json


def send(db_path, output_format):
    print("test")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='db_queue')
    data = {
        'db_path': db_path,
        'output_format': output_format
    }
    message = json.dumps(data)
    print("db_path: {}".format(data['db_path']))
    print("output_format: {}".format(data['output_format']))
    channel.basic_publish(exchange='',
                          routing_key='db_queue',
                          body=message)
    print(" [x] Sent to RabbitMQ")
    connection.close()
