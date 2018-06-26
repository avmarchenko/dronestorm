# -*- coding: utf-8 -*-
# Copyright 2018 Alex Marchenko
# Distributed under the terms of the Apache License 2.0
"""
Writing Cartesian Coordinates to the DB
############################################
This module provides a bolt that is responsible for writing
"""
from __future__ import division, print_function, absolute_import
import numpy as np
import numba as nb
import happybase as hb
from streamparse import Bolt
from dronedirector.distance import (lat_to_rad, lon_to_rad, earth_radius,
                                    to_cartesian, compute_distances)
from datetime import datetime


class DroneBolt(Bolt):
    """
    Stuff
    """
    def initialize(self, conf, ctx):
        """
        Tip:
            Method specific to streamparse.
        """
        self.logger.info(conf)

    def process(self, next_tuple):
        """
        Receives the next_tuple to process and processes it.

        Tip:
            Method specific to streamparse.
        """
        self.logger.info("Recieved: ", str(next_tuple))




#@nb.jit(nopython=True, nogil=True)
#def distance_one_many(x, y, z, xs, ys, zs):
#    """
#    Compute the distance between one object represented by the cartesian coordinates
#    (x, y, z) and a collection of objects represented by arrays {xs}, {ys}, {zs}.
#    
#    Args:
#    x (float): Cartesian x coordinate of source object
#    y (float): Cartesian x coordinate of source object
#    z (float): Cartesian x coordinate of source object
#    xs (array): Cartesian x coordinates of target objects
#    ys (array): Cartesian y coordinates of target objects
#    zs (array): Cartesian z coordinates of target objects
#    """
#    n = len(xs)
#    dxyz = np.empty((n, 4))
#    for i in range(n):
#        dx = (x - xs[i])
#        dy = (y - ys[i])
#        dz = (z - zs[i])
#        dr = np.sqrt(dx**2 + dy**2 + dz**2)
#        dxyz[i, 0] = dx
#        dxyz[i, 1] = dy
#        dxyz[i, 2] = dz
#        dxyz[i, 3] = dr
#    return dxyz
#    
#    
#class TestProximityBolt(Bolt):
#    outputs = ['datetime', 'id0', 'id1', 'proximity']
#
#    def initialize(self, conf, ctx):
#        """streamparse specific"""
#        self.conn = hb.Connection("10.0.0.13", port=16050)
#        self.cart = self.conn.table(b"drones_cart")
#        self.prox = self.conn.table(b"drones_proximity")
#
#    def process(self, tup):
#        """streamparse specific"""
#        id0 = tup.values[0]
#        id1 = tup.values[1]
#        alt0 = tup.values[2]
#        alt1 = tup.values[3]
#        lat0 = tup.values[4]
#        lat1 = tup.values[5]
#        lon0 = tup.values[6]
#        lon1 = tup.values[7]
#        r0 = get_sea_level(lat0) + alt0
#        x0, y0, z0 = to_cartesian(r0, lat0, lon0)
#        t0 = datetime.now()
#        rowkey0 = str.encode(id0 + t0.strftime(dtfmt))
#        self.cart.put(rowkey0, {b'uuid:uuid': str.encode(id0),
#                                b'datetime:datetime': str.encode(str(t0)),
#                                b'spatial:x': str.encode(str(x0)), 
#                                b'spatial:y': str.encode(str(y0)),
#                                b'spatial:z': str.encode(str(z0)),
#                                b'spatial:r': str.encode(str(r0))})
#        r1 = get_sea_level(lat1) + alt1
#        x1, y1, z1 = to_cartesian(r1, lat1, lon1)
#        t1 = datetime.now()
#        rowkey1 = str.encode(id1 + t1.strftime(dtfmt))
#        self.cart.put(rowkey1, {b'uuid:uuid': str.encode(id1),
#                                b'datetime:datetime': str.encode(str(t1)),
#                                b'spatial:x': str.encode(str(x1)), 
#                                b'spatial:y': str.encode(str(y1)),
#                                b'spatial:z': str.encode(str(z1)),
#                                b'spatial:r': str.encode(str(r1))})
#        dxyz = distance_one_many(x0, y0, z0, [x1], [y1], [z1])
#        t = datetime.now()
#        rowkey = str.encode(id0 + id1 + t.strftime(dtfmt))
#        self.prox.put(rowkey, {b'uuid:uuid0': str.encode(id0),
#                               b'uuid:uuid1': str.encode(id1),
#                               b'datetime:datetime': str.encode(str(t)),
#                               b'spatial:dx': str.encode(str(dxyz[0, 0])), 
#                               b'spatial:dy': str.encode(str(dxyz[0, 1])),
#                               b'spatial:dz': str.encode(str(dxyz[0, 2])),
#                               b'spatial:dr': str.encode(str(dxyz[0, 3]))})
#        if dxyz[0, 3] < 100:
#            self.logger.warn("{} {} {}: {}".format(t, id0, id1, dxyz[0, 3]))
#        self.emit([t.strftime(dtfmt), id0, id1, dxyz[0, 3]])
