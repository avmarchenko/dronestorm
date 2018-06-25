"""
NRTPD topology
"""

from streamparse import Grouping, Topology
from spouts.producer import TestCoordinateSpout
from bolts.consumer import TestProximityBolt


class NRTPDTopo(Topology):
    nrtpd_spout = TestCoordinateSpout.spec()
    nrtpd_bolt = TestProximityBolt.spec(inputs={nrtpd_spout: Grouping.fields('id0')}, par=2)
