#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import hashlib
import json
import sys
import uuid
import time
import os
import logging
import paho.mqtt.client as mqtt #import the client1
import glob

from autopial_lib.config_driver import ConfigFile
from autopial_lib.database_driver import DatabaseDriver
from autopial_lib.thread_worker import AutopialWorker
from autopial_lib.TorqueDriver import TorqueFileReader

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
steam_handler = logging.StreamHandler()
stream_formatter = logging.Formatter('%(asctime)s|%(levelname)08s | %(message)s')
steam_handler.setFormatter(stream_formatter)
logger.addHandler(steam_handler)

db = None

class AutopialSession:
    def __init__(self, origin):
        self.origin = origin
        self.session_uid = hashlib.md5(origin.encode('utf-8')).hexdigest()
        self.create()

    def create(self):
        logger.info("Creating session {} from {}".format(self.session_uid, self.origin))
        db.create_session(self.session_uid, self.origin)

    def start(self, start_date=datetime.datetime.now()):
        db.update_session(self.session_uid, start_date=start_date, status="ONGOING")

    def stop(self):
        db.update_session_metadata(self.session_uid)
        db.update_session(self.session_uid, status="TERMINATED")
        db.print_session(self.session_uid)

    def add_gps_location(self, fix, longitude, latitude, altitude, timestamp):
        db.add_gps_location(self.session_uid, fix, longitude, latitude, altitude, timestamp)

    def recreate(self):
        db.delete_session(self.session_uid)
        self.create()



class CheckFolder(AutopialWorker):
    def __init__(self, mqtt_client, time_sleep, folder_path):
        AutopialWorker.__init__(self, mqtt_client, time_sleep, logger=logger)
        self.folder_path = os.path.realpath(folder_path)

    def run(self):
        logger.info("CheckFolder thread starts")
        while self.wait():
            files = glob.glob(os.path.join(self.folder_path, "*.csv"))
            for csv_filepath in files:

                filename = os.path.basename(csv_filepath)
                lock_file = csv_filepath+".lock"
                done_file = csv_filepath + ".done"

                autopial_session = AutopialSession(filename)

                if os.path.exists(done_file):
                    logger.info("Ignoring file '{}' because already imported ('{}' exists)".format(csv_filepath, done_file))
                    continue

                if os.path.exists(lock_file):
                    logger.warning("Cannot lock file '{}' because it already exists".format(lock_file))
                    autopial_session.recreate()
                    os.remove(lock_file)

                torque_csv = TorqueFileReader(csv_filepath)
                os.mknod(lock_file)

                last_ts = 0
                autopial_session.start(start_date=torque_csv.start_date)
                for line in torque_csv.readline():
                    if (line["timestamp"] - last_ts) > 5:
                        gps_location = autopial_session.add_gps_location(latitude=line["latitude"],
                                                           longitude=line["longitude"],
                                                           altitude=line["altitude"],
                                                           fix=line["fix"],
                                                           timestamp=line["timestamp"])
                        last_ts = line["timestamp"]

                gps_location = autopial_session.add_gps_location(latitude=line["latitude"],
                                                   longitude=line["longitude"],
                                                   altitude=line["altitude"],
                                                   fix=line["fix"],
                                                   timestamp=line["timestamp"])


                autopial_session.stop()

                os.remove(lock_file)
                os.mknod(done_file)

        logger.info("CheckFolder thread ends")


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
        torque_path = cfg.get("torque_log", "path")
        check_every = cfg.get("torque_log", "check_every")
    except BaseException as e:
        logger.error("Invalid config file: {}".format(e))
        sys.exit(1)

    logger.info("Connecting to database: {}".format(database_path))
    db = DatabaseDriver(database=database_path, logger=logger)

    broker_address = "localhost"
    mqtt_client = mqtt.Client("sibus-logger")
    mqtt_client.on_message = on_message
    mqtt_client.connect(broker_address)
    mqtt_client.loop_start()
    mqtt_client.subscribe("autopial/#", qos=0)

    folder_checker = CheckFolder("TorqueFolder", time_sleep=check_every, folder_path=torque_path)
    folder_checker.start()

    try:
        while 1:
            time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        mqtt_client.loop_stop()
        folder_checker.stop()
