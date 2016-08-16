# Monitoring Tools for Storm Topologies

This module contains tools to monitor throughput and latency of Storm topologies.
We require our own tools, because the provided Storm tools are not sufficient for our purpose.
The prodived tooling can be used as an end-to-end monitoring with regard to evalution:
this implies, that we collect number in a way, such that we can use them as LaTeX input to automatically plot figures.
We also provide some bash scripts to collect data and assemlby LaTeX files that can be compiled to get a result PDF.

## Throughput

In order to monitor throughput of a spout/bolt, the operator can be wrapped using `ThroughputCounterSpout`/`ThroughputCounterBolt` from package `de.hus.cs.dbis.monitoring.throughput`.

## Latency

