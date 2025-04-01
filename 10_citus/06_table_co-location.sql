-- Consider the following tables which might be part of a multi-tenant web analytics SaaS
CREATE TABLE event (
tenant_id int,
event_id bigint,
page_id int,
payload jsonb,
primary key (tenant_id, event_id)
);
CREATE TABLE page (
tenant_id int,
page_id int,
path text,
primary key (tenant_id, page_id)
);

-- Dashboard Query
-- “Return the number of visits  all pages starting with ‘/blog’ in tenant two.”

-- Using regular postgres query
SELECT page_id, count(event_id)
FROM
page
LEFT JOIN event
USING (tenant_id, page_id)
WHERE tenant_id = 2 AND path LIKE '/blog%'
GROUP BY page_id;

-- Now let's shard it
-- naively use event_id and page_id as distribution columns
SELECT create_distributed_table('event', 'event_id');
SELECT create_distributed_table('page', 'page_id');

INSERT INTO event (tenant_id, event_id, page_id, payload) VALUES
(1, 1001, 10, '{"event_type": "click", "timestamp": "2025-04-01T10:00:00Z"}'),
(1, 1002, 11, '{"event_type": "view", "timestamp": "2025-04-01T10:05:00Z"}'),
(1, 1003, 12, '{"event_type": "purchase", "timestamp": "2025-04-01T10:10:00Z"}'),
(2, 1004, 13, '{"event_type": "click", "timestamp": "2025-04-01T10:15:00Z"}'),
(2, 1005, 14, '{"event_type": "view", "timestamp": "2025-04-01T10:20:00Z"}'),
(3, 1006, 15, '{"event_type": "purchase", "timestamp": "2025-04-01T10:25:00Z"}'),
(3, 1007, 16, '{"event_type": "click", "timestamp": "2025-04-01T10:30:00Z"}'),
(3, 1008, 17, '{"event_type": "view", "timestamp": "2025-04-01T10:35:00Z"}'),
(4, 1009, 18, '{"event_type": "purchase", "timestamp": "2025-04-01T10:40:00Z"}'),
(4, 1010, 19, '{"event_type": "click", "timestamp": "2025-04-01T10:45:00Z"}'),
(5, 1011, 20, '{"event_type": "view", "timestamp": "2025-04-01T10:50:00Z"}'),
(5, 1012, 21, '{"event_type": "purchase", "timestamp": "2025-04-01T10:55:00Z"}'),
(6, 1013, 22, '{"event_type": "click", "timestamp": "2025-04-01T11:00:00Z"}'),
(6, 1014, 23, '{"event_type": "view", "timestamp": "2025-04-01T11:05:00Z"}'),
(7, 1015, 24, '{"event_type": "purchase", "timestamp": "2025-04-01T11:10:00Z"}'),
(7, 1016, 25, '{"event_type": "click", "timestamp": "2025-04-01T11:15:00Z"}'),
(8, 1017, 26, '{"event_type": "view", "timestamp": "2025-04-01T11:20:00Z"}'),
(8, 1018, 27, '{"event_type": "purchase", "timestamp": "2025-04-01T11:25:00Z"}'),
(9, 1019, 28, '{"event_type": "click", "timestamp": "2025-04-01T11:30:00Z"}'),
(9, 1020, 29, '{"event_type": "view", "timestamp": "2025-04-01T11:35:00Z"}'),
(10, 1021, 30, '{"event_type": "purchase", "timestamp": "2025-04-01T11:40:00Z"}'),
(10, 1022, 31, '{"event_type": "click", "timestamp": "2025-04-01T11:45:00Z"}'),
(11, 1023, 32, '{"event_type": "view", "timestamp": "2025-04-01T11:50:00Z"}'),
(11, 1024, 33, '{"event_type": "purchase", "timestamp": "2025-04-01T11:55:00Z"}'),
(12, 1025, 34, '{"event_type": "click", "timestamp": "2025-04-01T12:00:00Z"}'),
(12, 1026, 35, '{"event_type": "view", "timestamp": "2025-04-01T12:05:00Z"}'),
(13, 1027, 36, '{"event_type": "purchase", "timestamp": "2025-04-01T12:10:00Z"}'),
(13, 1028, 37, '{"event_type": "click", "timestamp": "2025-04-01T12:15:00Z"}'),
(14, 1029, 38, '{"event_type": "view", "timestamp": "2025-04-01T12:20:00Z"}'),
(14, 1030, 39, '{"event_type": "purchase", "timestamp": "2025-04-01T12:25:00Z"}'),
(15, 1031, 40, '{"event_type": "click", "timestamp": "2025-04-01T12:30:00Z"}'),
(15, 1032, 41, '{"event_type": "view", "timestamp": "2025-04-01T12:35:00Z"}'),
(16, 1033, 42, '{"event_type": "purchase", "timestamp": "2025-04-01T12:40:00Z"}'),
(16, 1034, 43, '{"event_type": "click", "timestamp": "2025-04-01T12:45:00Z"}'),
(17, 1035, 44, '{"event_type": "view", "timestamp": "2025-04-01T12:50:00Z"}'),
(17, 1036, 45, '{"event_type": "purchase", "timestamp": "2025-04-01T12:55:00Z"}'),
(18, 1037, 46, '{"event_type": "click", "timestamp": "2025-04-01T13:00:00Z"}'),
(18, 1038, 47, '{"event_type": "view", "timestamp": "2025-04-01T13:05:00Z"}'),
(19, 1039, 48, '{"event_type": "purchase", "timestamp": "2025-04-01T13:10:00Z"}'),
(19, 1040, 49, '{"event_type": "click", "timestamp": "2025-04-01T13:15:00Z"}'),
(20, 1041, 50, '{"event_type": "view", "timestamp": "2025-04-01T13:20:00Z"}');

INSERT INTO page (tenant_id, page_id, path) VALUES
(1, 10, '/home'),
(1, 11, '/about'),
(1, 12, '/services'),
(2, 13, '/contact'),
(2, 14, '/blog'),
(3, 15, '/shop'),
(3, 16, '/cart'),
(3, 17, '/checkout'),
(4, 18, '/profile'),
(4, 19, '/settings'),
(5, 20, '/dashboard'),
(5, 21, '/orders'),
(6, 22, '/faq'),
(6, 23, '/terms'),
(7, 24, '/privacy'),
(7, 25, '/help'),
(8, 26, '/terms-and-conditions'),
(8, 27, '/refund-policy'),
(9, 28, '/store'),
(9, 29, '/products'),
(10, 30, '/services/consulting'),
(10, 31, '/services/development'),
(11, 32, '/team'),
(11, 33, '/careers'),
(12, 34, '/blog/2025'),
(12, 35, '/blog/2024'),
(13, 36, '/testimonials'),
(13, 37, '/reviews'),
(14, 38, '/partners'),
(14, 39, '/suppliers'),
(15, 40, '/news'),
(15, 41, '/events'),
(16, 42, '/contact-us'),
(16, 43, '/subscribe'),
(17, 44, '/feedback'),
(17, 45, '/customer-support'),
(18, 46, '/media'),
(18, 47, '/press'),
(19, 48, '/investors'),
(19, 49, '/sustainability'),
(20, 50, '/shop/new-arrivals');


-- Same above query don't work anymore
-- ERROR: complex joins are only supported when all distributed tables are co-located and joined on their distribution columns
SELECT page_id, count(event_id)
FROM
page
LEFT JOIN event
USING (tenant_id, page_id)
WHERE tenant_id = 2 AND path LIKE '/blog%'
GROUP BY page_id;

-- Let's recreate tables
DROP Table page;
DROP Table event;

-- run create table again
-- co-locate tables by using a common distribution column
SELECT create_distributed_table('event', 'tenant_id');
SELECT create_distributed_table('page', 'tenant_id', colocate_with => 'event');

-- Insert rows using above queries

-- Run dashboard query
SELECT page_id, count(event_id)
FROM
page
LEFT JOIN event
USING (tenant_id, page_id)
WHERE tenant_id = 2 AND path LIKE '/blog%'
GROUP BY page_id;
