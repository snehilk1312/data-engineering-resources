-- Check the table and tenant we need to move
SELECT company_id,count(*) FROM ads GROUP BY company_id ORDER BY 2 desc;

-- Let's move tenant with company_id -> 73 from one worker node to other 
-- First isolate the tenant’s data to a dedicated shard suitable to move. The CASCADE option also applies this change to
-- the rest of our tables distributed by company_id.
SELECT isolate_tenant_to_new_shard(
'ads', 73, 'CASCADE'
);
-- Output : 102435 , i.e new shard id where it has been isolated.


-- Next we move the data across the network to a new dedicated node
-- find the node currently holding the new shard
SELECT nodename, nodeport
FROM pg_dist_placement AS placement,
pg_dist_node AS node
WHERE placement.groupid = node.groupid
AND node.noderole = 'primary'
AND shardid = 102435;

-- Output : 10.134.178.137	5432

-- move the shard to your choice of worker (it will also move the
-- other shards created with the CASCADE option)
-- note that you should set wal_level for all nodes to be >= logical
-- to use citus_move_shard_placement.
-- you also need to restart your cluster after setting wal_level in
-- postgresql.conf files.
SELECT citus_move_shard_placement(
102435,
'10.134.178.137', 5432,
'10.134.178.163', 5432);

-- Rechecking where this shard exists now

SELECT nodename, nodeport
FROM pg_dist_placement AS placement,
pg_dist_node AS node
WHERE placement.groupid = node.groupid
AND node.noderole = 'primary'
AND shardid = 102435;

-- Output : 10.134.178.163	5432