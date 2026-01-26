# kafka-highway

the highway is a high-speed topic through which many messages pass.
it is traditionally (DDD) domain aligned, with no processing and minimal routing allowed.
off ramps are topics where routing and processing are available.
off ramps are intended to be organizationally aligned.
a given organization accepts ALL messages from a specific highway, but only processes a subset of them;
those unprocessed messages simply expire.
the organization can process as many of these messages as it sees fit within its fiefdom.
this may include performing further routing to deliver messages to downstream topics (that it owns).

## backlog

- start with POLICY 1 (sync publish) DONE
- move to POLICY 2 (outbox) IN-PROGRESS
- embed the topic within the owning DltPublisher
- enhance the shared contract JSON schemas to support namespaces
- modify the schema server to support query by namespace
- modify networknt 2.0.1 operations to query the CompiledRegistry
- move to POLICY 3 (Kafka transactions)
- enhanced shared contract (domain) classes (including canonical-product and order documents)
- move from properties within application.properties to database entries (persisted relationships)
