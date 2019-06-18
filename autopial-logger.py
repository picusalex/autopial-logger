#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
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

from haversine import haversine

from autopial_lib.config_driver import ConfigFile
from autopial_lib.Controller.CarSession import CarSession
from autopial_lib.SQLDatabaseDriver.sql_driver import DatabaseDriver
from autopial_lib.thread_worker import AutopialWorker
from autopial_lib.TorqueDriver import TorqueFileReader

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
steam_handler = logging.StreamHandler()
stream_formatter = logging.Formatter('%(asctime)s|%(levelname)08s | %(message)s')
steam_handler.setFormatter(stream_formatter)
logger.addHandler(steam_handler)

db_driver = None

class CheckFolder(AutopialWorker):
    def __init__(self, mqtt_client,
                 time_sleep,
                 folder_path,
                 file_pattern,
                 min_size=None,
                 max_size=None):
        AutopialWorker.__init__(self, mqtt_client, time_sleep, logger=logger)
        self.folder_path = os.path.realpath(folder_path)
        self.file_pattern = file_pattern
        self.min_size = min_size
        self.max_size = max_size

    def find_files(self, path=None):
        if path is None: path = self.folder_path
        files = []

        for i in os.listdir(path):
            full_i = os.path.join(path, i)
            if os.path.isdir(full_i):
                files.extend(self.find_files(full_i))
        files.extend(glob.glob(os.path.join(path, self.file_pattern)))
        return files

    def run(self):
        logger.info("CheckFolder thread starts")
        while self.wait():
            #files = glob.glob(os.path.join(self.folder_path, "*.csv"))
            files = self.find_files()

            for csv_filepath in files:
                logger.info("##########################################################################")
                logger.info("File found: {}".format(csv_filepath))

                filename = os.path.basename(csv_filepath)
                lock_file = csv_filepath+".lock"
                done_file = csv_filepath + ".done"

                if os.path.exists(done_file):
                    logger.info(" - file '{}' already imported ('{}' exists)".format(filename, done_file))
                    continue

                filesize = os.path.getsize(csv_filepath)
                if self.min_size is not None and filesize < self.min_size:
                    logger.warning(" - Ignore small file: {} (filesize < {})".format(filename, self.min_size))
                    continue

                if self.max_size is not None and filesize > self.max_size:
                    logger.warning(" - Ignore big file: {} (filesize > {})".format(filename, self.max_size))
                    continue

                torque_csv = TorqueFileReader(csv_filepath)
                if not torque_csv.isReady:
                    logger.error(" - error while opening CSV file '{}'".format(filename))
                    continue

                autopial_session = CarSession(origin=filename,
                                              db_driver=db_driver,
                                              logger=logger)

                if os.path.exists(lock_file):
                    logger.warning(" ! Cannot lock file '{}' because it already exists".format(lock_file))
                    autopial_session.recreate()
                    os.remove(lock_file)

                os.mknod(lock_file)

                last_ts = 0
                autopial_session.start(start_date=torque_csv.start_date)
                for line in torque_csv.readline():
                    if (line["timestamp"] - last_ts) > 1:
                        autopial_session.new_car_data(**line)
                        last_ts = line["timestamp"]

                autopial_session.new_car_data(**line)
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
        torque_pattern = cfg.get("torque_log", "pattern")
        check_every = cfg.get("torque_log", "check_every")
        min_size = cfg.get("torque_log", "min_size")
        if min_size.endswith("k"):
            min_size = float(min_size.strip("k"))*1024
        elif min_size.endswith("M"):
            min_size = float(min_size.strip("M"))*1024*1024

        max_size = cfg.get("torque_log", "max_size")
        if max_size.endswith("k"):
            max_size = float(max_size.strip("k"))*1024
        elif max_size.endswith("M"):
            max_size = float(max_size.strip("M"))*1024*1024
    except BaseException as e:
        logger.error("Invalid config file: {}".format(e))
        sys.exit(1)

    logger.info("Connecting to database: {}".format(database_path))
    db_driver = DatabaseDriver(database=database_path,
                               logger=logger)

    broker_address = "localhost"
    mqtt_client = mqtt.Client("sibus-logger")
    mqtt_client.on_message = on_message
    mqtt_client.connect(broker_address)
    mqtt_client.loop_start()
    mqtt_client.subscribe("autopial/#", qos=0)

    folder_checker = CheckFolder("TorqueFolder",
                                 time_sleep=check_every,
                                 folder_path=torque_path,
                                 file_pattern=torque_pattern,
                                 min_size=min_size,
                                 max_size=max_size)
    folder_checker.start()

    try:
        while 1:
            time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        mqtt_client.loop_stop()
        folder_checker.stop()
