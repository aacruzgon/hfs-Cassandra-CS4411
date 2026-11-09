-- Helios Cassandra backend: keyspace for the three-node cluster
-- (docker-compose.cluster.yml, roadmap item 0.3). Apply it instead of
-- docs/Cassandra/schema/keyspace-single-node.cql, then apply schema.cql:
--
--   cqlsh -f keyspace-three-node.cql
--   cqlsh -k helios -f schema.cql
--
-- The same CREATE KEYSPACE as keyspace-single-node.cql with 'helios-dc1': 3.
-- That file's system_traces ALTER is not repeated: three nodes satisfy its
-- default replication factor 2.

CREATE KEYSPACE IF NOT EXISTS helios
    WITH replication = {'class': 'NetworkTopologyStrategy', 'helios-dc1': 3};
