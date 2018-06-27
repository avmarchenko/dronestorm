# -*- coding: utf-8 -*-
# Copyright 2018 Alex Marchenko
# Distributed under the terms of the Apache License 2.0
"""
Simple Drone Storm Spout
##########################
A Storm spout is in effect a message producer for Storm. This module provides a
Spout that reads from a Kafka topic performs a database update, and sends the data
to Storm for stream processing.
"""
from __future__ import division, print_function, absolute_import
import json
from streamparse import Spout
from datetime import datetime
from confluent_kafka import Consumer


class SimpleDroneSpout(Spout):
    """
    A Storm spout that receives messages from a Kafka broker, sends them for proximity
    monitoring, and records its actions.

    Tip:
        The configuration file, internal.json (see config/internal.json.example) must
        be present in the root of this repository 
    """
    outputs = ['uid', 'dronetime', 'region', 'altitude', 'latitude', 'longitude']

    def initialize(self, stormconf, context):
        """
        Additional instantiation of our spout (Storm producer). Our Kafka consumer is
        initialized here.

        Note:
            Method internal to streamparse used to setup the spout.

        See Also:
            https://github.com/avmarchenko/delivery-drones
        """
        with open("/opt/internal.json") as f:
            config = json.load(f)
        self.internal_config = config
        self.logger.warn("Internal Config: " + str(config))
        self.logger.warn("Storm Config: " + str(stormconf))
        self.logger.warn("Storm Context: "+ str(context))
        self.timeout = float(config['timeout'])
        # Our kafka consumer client
        self.ckc = Consumer({'bootstrap.servers': config['kafka'], 'group.id': config['kfgroupid'],
                             'default.topic.config': {'auto.offset.reset': "smallest"}})
        self.ckc.subscribe([config['raw_topic']])

    def next_tuple(self):
        """
        Called continuously to process stream messages.

        Note:
            Method internal to streamparse that follows Storm's API.
        """
        msg = self.ckc.poll(self.timeout)
        if msg is not None:
            text = msg.value().decode("utf-8")
            if "uid" in text:
                data = json.loads(text)
                self.emit([data['uid'], data['dronetime'], data['region'], data['altitude'],
                           data['latitude'], data['longitude']])
