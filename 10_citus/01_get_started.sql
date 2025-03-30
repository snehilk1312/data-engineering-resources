-- Create the tables
CREATE TABLE companies (
id bigint NOT NULL,
name text NOT NULL,
image_url text,
created_at timestamp without time zone NOT NULL,
updated_at timestamp without time zone NOT NULL
);
CREATE TABLE campaigns (
id bigint NOT NULL,
company_id bigint NOT NULL,
name text NOT NULL,
cost_model text NOT NULL,
state text NOT NULL,
monthly_budget bigint,
blacklisted_site_urls text[],
created_at timestamp without time zone NOT NULL,
updated_at timestamp without time zone NOT NULL
);
CREATE TABLE ads (
id bigint NOT NULL,
company_id bigint NOT NULL,
campaign_id bigint NOT NULL,
name text NOT NULL,
image_url text,
target_url text,
impressions_count bigint DEFAULT 0,
clicks_count bigint DEFAULT 0,
created_at timestamp without time zone NOT NULL,
updated_at timestamp without time zone NOT NULL
);

-- Add primary key
ALTER TABLE companies ADD PRIMARY KEY (id);
ALTER TABLE campaigns ADD PRIMARY KEY (id, company_id);
ALTER TABLE ads ADD PRIMARY KEY (id, company_id);

-- Distribute/Shard tables on a column
SELECT create_distributed_table('companies', 'id');
SELECT create_distributed_table('campaigns', 'company_id');
SELECT create_distributed_table('ads', 'company_id');

-- Loading the data in table
copy companies from '/tmp/companies.csv' with csv;
copy campaigns from '/tmp/campaigns.csv' with csv;
copy ads from '/tmp/ads.csv' with csv;

-- Running Queries

-- Insert
INSERT INTO companies VALUES (5000, 'Snehil', 'https://randomurl/image.png', now(),now());

-- Select
SELECT * from companies where name='Snehil';

-- Update
UPDATE companies set name='random' where name='Snehil';