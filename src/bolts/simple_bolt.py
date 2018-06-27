# -*- coding: utf-8 -*-
# Copyright 2018 Alex Marchenko
# Distributed under the terms of the Apache License 2.0
"""
Simple Drone Storm Bolt
##########################
This module provides a Storm bolt that converts altitude, latitude, and longitude
associated with a given aerial object (tuple message) to Cartesian coordiantes so
that distances can be computed.
"""
import json
import numpy as np
import numba as nb
import happybase as hb
from collections import OrderedDict
from streamparse import Bolt
from datetime import datetime
from dronedirector.aerial import dtfmt
from dronedirector.distance import lat_to_rad, lon_to_rad, to_cartesian, compute_distances


class FixedLenDict(OrderedDict):
    """
    An ordered dictionary of fixed maximum size.

    See Also:
        https://gist.github.com/bencharb/729971d4a9e4633ea08a
    """
    _default_max_size = 2000

    def update(self, **kwargs):
        """Thin wrapper around OrderedDict.update."""
        super(FixedLenDict, self).update(**kwargs)
        self._prune(self.max_size)
    
    def _prune_dict(self, max_size):
        """Reduce the dictionary to the size limit."""
        if len(self) >= max_size:
            diff = len(self) - max_size 
            for k in self.keys()[:diff]:
                del self[k]

    def __setitem__(self, key, val):
        if key not in self and len(self) == self.max_size:
            max_size = self.max_size - 1                  # Set custom size temporarily
            self._prune_dict(max_size)                    # And prune the oldest
        super(FixedLenDict, self).__setitem__(key, val)     # Then add the new data (and restore the size)

    def __init__(self, *args, **kwargs):
        self.max_size = kwargs.pop('max_size', self._default_max_size)
        super(FixedLenDict, self).__init__(*args, **kwargs)
    
    
class SimpleDroneBolt(Bolt):
    """
    A Storm bolt that processes (all in one) and records data coming from
    simulated delivery drones.
    """
    def initialize(self, conf, ctx):
        """
        Method specific to streamparse -- used to configure DB connections.

        See Also:
            https://github.com/avmarchenko/delivery-drones
        """
        with open("/opt/internal.json") as f:
            config = json.load(f)
        self.current = FixedLenDict(max_size=config['density'])
        self.conn = hb.Connection(config['hbase'], port=int(config['thrift']))
        self.raw = self.conn.table(str.encode(config['raw_table']))
        self.cart = self.conn.table(str.encode(config['cart_table']))
        self.prox = self.conn.table(str.encode(config['prox_table']))

    def _compute_dist(self):
        """A thin wrapper around compute_distances from dronedirector."""
        x, y, z = list(zip(*list(self.current.values())))
        return compute_distances(x, y, z)

    def process(self, tup):
        """All in one stream processing; conversion, calculation, and DB update."""
        self.logger.info(str(tup))
        uid = tup.values[0]
        dronetime = tup.values[1]
        region = tup.values[2]
        altitude = tup.values[3]
        latitude = tup.values[4]
        longitude = tup.values[5]
        pk = uid + dronetime    # String concat
        # Write raw data to HBase
        self.raw.put(pk, {b'uuid:uuid': str.encode(uid),
                          b'datetime:dronetime': str.encode(dronetime),
                          b'spatial:alt': str.encode(str(altitude)), 
                          b'spatial:lat': str.encode(str(latitude)),
                          b'spatial:lon': str.encode(str(longitude))})
        # Compute Cartesian
        lat = lat_to_rad(latitude)
        lon = lon_to_rad(longitude)
        x, y, z, r = to_cartesian(altitude, lat, lon)
        self.raw.put(pk, {b'uuid:uuid': str.encode(uid),
                          b'datetime:carttime': str.encode(datetime.now().strftime(dtfmt)),
                          b'spatial:x': str.encode(str(x)), 
                          b'spatial:y': str.encode(str(y)),
                          b'spatial:z': str.encode(str(z)),
                          b'spatial:r': str.encode(str(r))})
        # Add the data to our current drones and compute
        self.current[uid] = (x, y, z)
        dxyz = self._compute_dist()
        self.logger.info("current: " + str(self.current))
        self.logger.info("current: " + str(len(self.current)))
        self.logger.info("dxyz:" + str(dxyz))
        t = datetime.now().strftime(dtfmt)
        k = 0
        keys = list(self.current.keys())   # Because of how dxyz is return
        n = len(keys)                      # we iterate over keys in this
        for ii in range(n):                # manner.
            i = keys[ii]                   # i is the value of the iith key
            for jj in range(ii + 1, n):
                j = keys[jj]               # j is the value of the jjth key
                self.logger.info("I: " + i + "    J :" + j + "    k: " + str(k))
                d = dxyz[k, :]
                if d[3] < 100:
                    self.logger.warn("Drones {}, {} are {} meters apart!".format(i, j, str(d[3])))
                pkk = i + j + t
                self.prox.put(pkk, {b'uuid:uuid0': str.encode(i),
                                    b'uuid:uuid1': str.encode(j),
                                    b'datetime:datetime': str.encode(str(t)),
                                    b'spatial:dx': str.encode(str(d[0])), 
                                    b'spatial:dy': str.encode(str(d[1])),
                                    b'spatial:dz': str.encode(str(d[2])),
                                    b'spatial:dr': str.encode(str(d[3]))})
                k += 1
