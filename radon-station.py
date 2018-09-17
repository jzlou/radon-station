#!/usr/bin/env python3
"""Simple logging radon station logic"""
import sys
import os
import struct
import logging
from datetime import datetime
import pika
import json
from bluepy.blte import UUID, Peripheral

if len(sys.argv) > 1:
    LOCATION = sys.argv[1]
else:
    LOCATION = 'basement'
logging.info("using %s as location", LOCATION)

CONNECTION = pika.BlockingConnection(pika.ConnectionParameters(
    host='lecole',
    port=5672
))
CHANNEL = CONNECTION.channel()
CHANNEL.queue_declare(queue='scribe')

WAVE_ADDRESS = os.environ['WAVE_ADDRESS']


class Sensor:
    def __init__(self, name, uuid, format_type, unit, scale):
        self.name = name
        self.uuid = uuid
        self.format_type = format_type
        self.unit = unit
        self.scale = scale


def get_data():
    p = Peripheral(WAVE_ADDRESS)
    sensors = []
    sensors.append(Sensor("temperature", UUID(0x2A6E),
                          'h', "deg C\t", 1.0/100.0))
    sensors.append(Sensor("humidity", UUID(0x2A6F),
                          'H', "%\t\t", 1.0/100.0))
    sensors.append(Sensor("radiation",
                          "b42e01aa-ade7-11e4-89d3-123b93f75cba",
                          'H', "Bq/m3\t", 1.0))
    data = {}
    for s in sensors:
        characteristic = p.getCharacteristics(uuid=s.uuid)[0]
        if (characteristic.supportsRead()):
            value = characteristic.read()
            value = struct.unpack(s.format_type, value)
            data[s.name] = value
    p.disconnect()
    logging.info('data received:' data)
    return data


def write_point(datum):
    """Writes the point to influx"""
    measurement = {
            "measurement": "weather",
            "tags": {
                "location": LOCATION
                },
            "time": datetime.now().isoformat(),
            "fields": datum
            }
    CHANNEL.basic_publish(exchange='',
                          routing_key='scribe',
                          body=json.dumps(measurement))


if __name__ == "__main__":
    try:
        write_point(get_data())
    except Exception:
        raise RuntimeError("Failed in main loop")
    print("Wrote data to tsdb")
