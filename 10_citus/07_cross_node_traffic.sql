-- In multi-tenant apps using Citus, missing tenant_id(distribution key) filters can cause queries to hit all shards, wasting resources. 
-- Enable logging of multi-shard queries via config to catch these issues early.

-- adjust for your own database's name of course
ALTER DATABASE postgres SET citus.multi_task_query_log_level = 'error';

-- log instead error
ALTER DATABASE postgres SET citus.multi_task_query_log_level = 'log';