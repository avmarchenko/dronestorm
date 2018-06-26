# -*- coding: utf-8 -*-
# Copyright 2018 Alex Marchenko
# Distributed under the terms of the Apache License 2.0
"""
Drone Storm Topology
########################
"""
from __future__ import print_function, absolute_import
from streamparse import Topology
from spouts.kafka_spout import DroneSpout
from bolts.cart_bolt import CartesianBolt


class DroneTopology(Topology):
    """
    Definition of the Storm topology.
    """
    drone_spout = DroneSpout.spec(name='drone-spout')
    cart_bolt = CartesianBolt.spec(name='cartesian-bolt', inputs=[drone_spout])




#from spouts.producer import TestCoordinateSpout
#from bolts.consumer import TestProximityBolt


#class NRTPDTopo(Topology):
#    nrtpd_spout = TestCoordinateSpout.spec()
#    nrtpd_bolt = TestProximityBolt.spec(inputs={nrtpd_spout: Grouping.fields('id0')}, par=2)
