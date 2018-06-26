# -*- coding: utf-8 -*-
# Copyright 2018 Alex Marchenko
# Distributed under the terms of the Apache License 2.0
"""
Drone Storm Spout
##################
A Storm spout is in effect a message producer for Storm. This module provides a
Spout that reads from a Kafka topic performs a database update, and sends the data
to Storm for stream processing.
"""
from __future__ import division, print_function, absolute_import
import json
from streamparse import Spout, Stream
from datetime import datetime
#from dronedirector.aerial import dtfmt
from confluent_kafka import Consumer
#from confluent_kafka.avro import AvroConsumer


class DroneSpout(Spout):
    """
    A Storm spout that receives messages from a Kafka broker, sends them for proximity
    monitoring, and records its actions.

    Tip:
        The configuration file, internal.json (see config/internal.json.example) must
        be present in the root of this repository 
    """
    outputs = [Stream(name='cartesian-bolt', fields=['pk', "altitude", "latitude", "longitude"]),
               Stream(fields=['uid', 'altitude', 'latitude', 'longitude', 'dronetime', 'region'])]
    #outputs = ['pk', 'altitude', 'latitude', 'longitude']
    #outputs = [Stream(name='cartesian-bolt', fields=['pk', 'alt', 'lat', 'lon'])]
               #Stream(name='write-raw-bolt', fields=['uid'])]

    def initialize(self, stormconf, context):
        """
        Additional instantiation of our spout (Storm producer). Our Kafka consumer is
        initialized here.

        Note:
            Method internal to streamparse used to setup the spout.
        """
        self.logger.warn(os.path.dirname(os.path.realpath(__file__)))
        with open("/opt/internal.json") as f:
            config = json.load(f)
        self.internal_config = config
        self.logger.warn("Internal Config: " + str(config))
        self.logger.warn("Storm Config: " + str(stormconf))
        self.logger.warn("Storm Context: "+ str(context))
        # Our kafka consumer client
        self.timeout = float(config['timeout'])
        self.ckc = Consumer({'bootstrap.servers': config['kafka'], 'group.id': config['kfgroupid'],
                             'default.topic.config': {'auto.offset.reset': "smallest"}})
        self.ckc.subscribe([config['raw_topic']])
        self.conn = hb.Connection(config['hbase'], port=int(config['thrift']))
        self.raw = self.conn.table(str.encode(config['raw_table']))

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
                pk = data['uid'] + data['dronetime']
                self.emit([pk, msg['altitude'], msg['latitude'] msg['longitude'], stream="cartesian-bolt")


#class TestCoordinateSpout(Spout):
#    outputs = ['id0', 'id1', 'alt0', 'alt1', 'lat0', 'lat1', 'lon0', 'lon1', 't0', 't1']
#
#    def initialize(self, stormconf, context):
#        self.lon = cycle(np.sin(np.arange(0, 10*np.pi, np.pi/(360*50))))
#        self.lon_ = cycle(np.sin(np.arange(0, 20*np.pi, np.pi/(360*25))))
#        self.lat = cycle([0.7])    #cycle(np.arange(0.69, 0.72, 0.001))
#        self.alt = cycle([1000.0])    #cycle([900.0, 950.0, 1000.0, 1100.0])
#        rd = random.Random()
#        rd.seed(0)
#        self.id0 = uuid.UUID(int=rd.getrandbits(128))
#        rd.seed(1)
#        self.id1 = uuid.UUID(int=rd.getrandbits(128))
#        self.conn = hb.Connection("10.0.0.13", port=16050)
#        self.raw = self.conn.table(b"drones_raw")
#
#    def next_tuple(self):
#        t0 = datetime.now()
#        id0 = self.id0.hex
#        id1 = self.id1.hex
#        alt0 = next(self.alt)
#        alt1 = next(self.alt)
#        lat0 = next(self.lat)
#        lat1 = next(self.lat)
#        lon0 = next(self.lon)
#        lon1 = next(self.lon_)
#        t1 = datetime.now()
#        rowkey0 = str.encode(id0 + t0.strftime(dtfmt))
#        self.raw.put(rowkey0, {b'uuid:uuid': str.encode(id0),
#                               b'datetime:datetime': str.encode(str(t0)),
#                               b'spatial:alt': str.encode(str(alt0)), 
#                               b'spatial:lat': str.encode(str(lat0)),
#                               b'spatial:lon': str.encode(str(lon0))})
#        rowkey1 = str.encode(id1 + t1.strftime(dtfmt))
#        self.raw.put(rowkey1, {b'uuid:uuid': str.encode(id1),
#                               b'datetime:datetime': str.encode(str(t1)),
#                               b'spatial:alt': str.encode(str(alt1)), 
#                               b'spatial:lat': str.encode(str(lat1)),
#                               b'spatial:lon': str.encode(str(lon1))})
#        self.emit([id0, id1, alt0, alt1, lat0, lat1, lon0, lon1, t0.strftime(dtfmt), t1.strftime(dtfmt)])
