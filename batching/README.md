## Aeolus' Batching Component

Aeolus' batching component is a transparent layer on top of Apache Storm that is able to increase the throughput by an order of magnitude. The layer is transparent in two ways:
 1. Storm does not notice the layer because the used batches look like regular (fat) tuples to the system.
 2. The layer in transparent to the topology, ie, already implemented Spouts and Bolts must not be changed.

Because batching is transparent to the system and to the user code, tuple-by-tuple processing semantics are preserved. Furthermore, the batch size can be set differently for each producer-consumer pair, allowing to keep processing latency low.

Current limitations: no fault-tolerance support (ie, anchoring and acking calls are dropped) [work in progress]


### Usage

Batching can be used manually by using the provided Spout- and Bolt-Wrapper classes while building a topology. This give the user the most control over the batching layer and minimizes overhead.

Additionally, the Storm provided `TopologyBuilder` can be replaced by `AeolusBuilder` (alpha version), which offers the same interface. Furthermore, for each Spout or Bolt, the user is able to specify the output batch size to be used.

