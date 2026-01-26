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

- start with POLICY 1 (sync publish) CHECK
- move to POLICY 2 (outbox) IN-PROGRESS
- move to POLICY 3 (Kafka transactions)
- move from application.properties properties to database entries (persisted relationships)
