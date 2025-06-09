
# Amazon Redshift Join Distribution Strategies (Query Plan Terms)

This guide explains internal Redshift join distribution strategies (`DS_` terms) seen in `EXPLAIN` query plans. These indicate how data is moved across nodes to execute joins efficiently.

---

## ✅ 1. DS_DIST_NONE

- **Meaning**: No redistribution needed.
- **When**: 
  - Tables share the same `DISTKEY` and join on that key.
  - One table uses `DISTSTYLE ALL`.
- **Best Case**: Join happens locally on each node (fastest).

**Example**:

```sql
CREATE TABLE orders (
  order_id INT,
  customer_id INT
) DISTSTYLE KEY DISTKEY (customer_id);

CREATE TABLE customers (
  customer_id INT,
  name TEXT
) DISTSTYLE KEY DISTKEY (customer_id);

EXPLAIN
SELECT * FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
```

---

## ✅ 2. DS_DIST_KEY

- **Meaning**: One table is redistributed based on the join key.
- **When**: Only one table has the right `DISTKEY`.

**Example**:

```sql
CREATE TABLE events (
  event_id INT,
  customer_id INT
) DISTSTYLE EVEN;

EXPLAIN
SELECT * FROM orders o
JOIN events e ON o.customer_id = e.customer_id;
```

`events` is redistributed on `customer_id`.

---

## ✅ 3. DS_DIST_ALL_NONE

- **Meaning**: One table (usually small) is replicated to all nodes (`DISTSTYLE ALL`).
- **When**: Joining a large table with a small replicated dimension table.

**Example**:

```sql
CREATE TABLE countries (
  country_code CHAR(2),
  name TEXT
) DISTSTYLE ALL;

EXPLAIN
SELECT * FROM orders o
JOIN countries c ON o.country_code = c.country_code;
```

---

## ✅ 4. DS_DIST_INNER

- **Meaning**: Inner table is redistributed based on the join key.
- **When**: The inner table is not co-located with the outer table.

**Example**:

```sql
CREATE TABLE returns (
  order_id INT
) DISTSTYLE EVEN;

EXPLAIN
SELECT * FROM returns r
JOIN orders o ON r.order_id = o.order_id;
```

Depending on join order, `orders` may be redistributed (`DS_DIST_INNER`).

---

## ✅ 5. DS_DIST_BOTH

- **Meaning**: Both tables are redistributed because neither has aligned distribution.
- **Worst Case**: High network and CPU overhead.

**Example**:

```sql
CREATE TABLE table1 (
  id1 INT,
  x TEXT
) DISTSTYLE EVEN;

CREATE TABLE table2 (
  id2 INT,
  y TEXT
) DISTSTYLE EVEN;

EXPLAIN
SELECT * FROM table1 t1
JOIN table2 t2 ON t1.id1 = t2.id2;
```

Both tables are moved → `DS_DIST_BOTH`.

---

## ✅ 6. DS_BCAST_INNER

- **Meaning**: Redshift broadcasts (copies) the small inner table to all nodes.
- **When**: Inner table is small enough to broadcast even if not `DISTSTYLE ALL`.

**Example**:

```sql
CREATE TABLE small_dim (
  dim_id INT,
  value TEXT
) DISTSTYLE EVEN;

EXPLAIN
SELECT * FROM big_fact bf
JOIN small_dim sd ON bf.dim_id = sd.dim_id;
```

`small_dim` is broadcast → `DS_BCAST_INNER`.

---

## ✅ 7. No Redistribution

- **Meaning**: No data movement required.
- **Occurs In**: 
  - `DS_DIST_NONE`
  - `DS_DIST_ALL_NONE`
  - Optimized queries with co-located or broadcasted joins

---

## 🔁 Summary Table

| Term               | Redistribution | When It Happens                                                  |
|--------------------|----------------|------------------------------------------------------------------|
| `DS_DIST_NONE`     | ❌ None         | Same `DISTKEY` or `DISTSTYLE ALL`                               |
| `DS_DIST_KEY`      | ✅ One Table    | One table has proper `DISTKEY`, the other doesn't               |
| `DS_DIST_ALL_NONE` | ❌ None         | One table is replicated with `DISTSTYLE ALL`                    |
| `DS_DIST_INNER`    | ✅ Inner Table  | Inner table is misaligned with join key                         |
| `DS_DIST_BOTH`     | ✅ Both Tables  | Neither table is correctly distributed                          |
| `DS_BCAST_INNER`   | ✅ Broadcast    | Inner table is small and broadcast to all nodes                 |
| No Redistribution  | ❌ None         | Data is already aligned for the join                            |

--- 
One distribution plan can change into other depending on cluster configuration,size,etc.
- eg:  DS_DIST_BOTH  -> DS_BCAST_INNER