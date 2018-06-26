# -*- coding: utf-8 -*-
# Copyright 2018 Alex Marchenko
# Distributed under the terms of the Apache License 2.0
"""
Drone Storm Topology
########################
"""
from __future__ import print_function, absolute_import
from streamparse import Grouping, Topology


class DroneTopology(Topology):
    """
    """
#from spouts.producer import TestCoordinateSpout
#from bolts.consumer import TestProximityBolt


#class NRTPDTopo(Topology):
#    nrtpd_spout = TestCoordinateSpout.spec()
#    nrtpd_bolt = TestProximityBolt.spec(inputs={nrtpd_spout: Grouping.fields('id0')}, par=2)
