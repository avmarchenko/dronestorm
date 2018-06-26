# dronestorm
A simple Storm topology for processing the proximity of drones

# Usage

# Configuration

An example streamparse config.json file is provided here. It should be place in the root directory of this repository (e.g. soft-linked) and renamed to config.json. Additional information on this config file can be found [here](https://streamparse.readthedocs.io/en/stable/). The field "hbport" is used internally to connect to HBase and is not part of streamparse's configuration. It is assumed that the "nimbus" host is the same host for HBase's thrift server.
