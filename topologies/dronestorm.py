# -*- coding: utf-8 -*-
# Copyright 2018 Alex Marchenko
# Distributed under the terms of the Apache License 2.0
"""
Drone Storm Topology
########################
"""
from __future__ import print_function, absolute_import
from streamparse import Topology
from spouts.simple_spout import SimpleDroneSpout
from bolts.simple_bolt import SimpleDroneBolt


class SimpleDroneTopology(Topology):
    """Definition of the Storm topology."""
    drone_spout = SimpleDroneSpout.spec()
    drone_bolt = SimpleDroneBolt.spec(inputs=[drone_spout])
