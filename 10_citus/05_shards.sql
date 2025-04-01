-- The pg_dist_shard metadata table on the coordinator contains a row for each shard of each distributed table in the
-- system. The row matches a shardid with a range of integers in a hash space (shardminvalue, shardmaxvalue).
-- If the coordinator node wants to determine which shard holds a row of github_events, it hashes the value of the
-- distribution column in the row, and checks which shard’s range contains the hashed value.
SELECT * from pg_dist_shard;

-- Shard Placement
SELECT
shardid,
node.nodename,
node.nodeport
FROM pg_dist_placement placement
JOIN pg_dist_node node
ON placement.groupid = node.groupid
AND node.noderole = 'primary'::noderole
WHERE shardid = 102234;