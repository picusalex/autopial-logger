#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

import sys
import threading
import uuid

import time
from flask import Flask, render_template, Response, jsonify, send_file, abort
import logging
import paho.mqtt.client as mqtt #import the client1
from flask_socketio import SocketIO, join_room, leave_room
import redis

from autopial_lib.config_driver import ConfigFile
from autopial_lib.database_driver import DatabaseDriver

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
steam_handler = logging.StreamHandler()
stream_formatter = logging.Formatter('%(asctime)s|%(levelname)08s | %(message)s')
steam_handler.setFormatter(stream_formatter)
logger.addHandler(steam_handler)


def on_message(client, userdata, message):
    payload_str = message.payload.decode("utf-8")
    payload = json.loads(payload_str)

    autopial_device_name = payload["autopial"]["device_name"]
    autopial_device_uid = payload["autopial"]["device_uid"]
    autopial_process_name = payload["autopial"]["process_name"]
    autopial_worker_name = payload["autopial"]["worker_name"]



    pass

if __name__ == '__main__':
    cfg = ConfigFile("autopial-logger.cfg", logger=logger)
    try:
        database_path = cfg.get("database", "path")
    except BaseException as e:
        logger.error("Invalid config file: {}".format(e))
        sys.exit(1)

    db = DatabaseDriver(database=database_path)

    broker_address = "localhost"
    mqtt_client = mqtt.Client("sibus-logger-{}".format(uuid.uuid4().hex))
    mqtt_client.on_message = on_message
    mqtt_client.connect(broker_address)
    mqtt_client.loop_start()
    mqtt_client.subscribe("autopial/#", qos=0)

    try:
        while 1:
            time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        mqtt_client.loop_stop()
