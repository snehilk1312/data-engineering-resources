-- Create demo github events and users tables

CREATE TABLE github_events
(
event_id bigint,
event_type text,
event_public boolean,
repo_id bigint,
payload jsonb,
repo jsonb,
user_id bigint,
org jsonb,
created_at timestamp
);

CREATE TABLE github_users
(
user_id bigint,
url text,
login text,
avatar_url text,
gravatar_id text,
display_login text
);


-- Create index,for jsonb type column we are using GIN index(instead B-Tree)
CREATE INDEX event_type_index ON github_events (event_type);
CREATE INDEX payload_index ON github_events USING GIN (payload jsonb_path_ops);


-- Distribute tables
SELECT create_distributed_table('github_users', 'user_id');
SELECT create_distributed_table('github_events', 'user_id');

-- Load data
copy github_users from '/tmp/users.csv' with csv;
copy github_events from '/tmp/events.csv' with csv;

-- Running queries
SELECT count(*) FROM github_users;

-- number of commits per minute by using
the number of distinct commits in each push event
SELECT date_trunc('minute', created_at) AS minute,
sum((payload->>'distinct_size')::int) AS num_commits
FROM github_events
WHERE event_type = 'PushEvent'
GROUP BY minute
ORDER BY minute desc;

-- top 10 users with most created repository
SELECT login, count(*)
FROM github_events ge
JOIN github_users gu
ON ge.user_id = gu.user_id
WHERE event_type = 'CreateEvent' AND payload @> '{"ref_type": "repository"}'
GROUP BY login
ORDER BY count(*) DESC LIMIT 10;