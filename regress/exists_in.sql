-- Minimal regression set for EXISTS/IN/NOT EXISTS/NOT IN behavior.

CREATE DATABASE IF NOT EXISTS shiro_regress;
USE shiro_regress;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (
  id INT PRIMARY KEY,
  v INT
);

CREATE TABLE t2 (
  id INT,
  v INT
);

CREATE TABLE t3 (
  id INT,
  v INT
);

INSERT INTO t1 (id, v) VALUES
  (1, 10),
  (2, 20),
  (3, NULL);

INSERT INTO t2 (id, v) VALUES
  (1, 10),
  (2, NULL),
  (4, 40);

INSERT INTO t3 (id, v) VALUES
  (2, 20),
  (3, 30);

-- Correlated EXISTS: expect ids 1,2.
SELECT 'q1_exists' AS case_name, t1.id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id) ORDER BY t1.id;

-- Correlated NOT EXISTS: expect id 3.
SELECT 'q2_not_exists' AS case_name, t1.id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id) ORDER BY t1.id;

-- IN subquery: expect ids 1,2.
SELECT 'q3_in_subq' AS case_name, t1.id FROM t1 WHERE t1.id IN (SELECT id FROM t2) ORDER BY t1.id;

-- NOT IN subquery with NULL present in subquery: expect empty result.
SELECT 'q4_not_in_subq_null' AS case_name, t1.id FROM t1 WHERE t1.id NOT IN (SELECT v FROM t2) ORDER BY t1.id;

-- NOT IN subquery with NULL filtered out: expect id 3.
SELECT 'q5_not_in_subq_no_null' AS case_name, t1.id FROM t1 WHERE t1.id NOT IN (SELECT id FROM t2 WHERE id IS NOT NULL) ORDER BY t1.id;

-- IN list: expect ids 1,3.
SELECT 'q6_in_list' AS case_name, t1.id FROM t1 WHERE t1.id IN (1, 3) ORDER BY t1.id;

-- NOT IN list: expect id 2.
SELECT 'q7_not_in_list' AS case_name, t1.id FROM t1 WHERE t1.id NOT IN (1, 3) ORDER BY t1.id;

-- EXISTS with value predicate: expect id 1 (t2.v matches t1.v).
SELECT 'q8_exists_value' AS case_name, t1.id FROM t1 WHERE EXISTS (
  SELECT 1 FROM t2 WHERE t2.id = t1.id AND t2.v = t1.v
) ORDER BY t1.id;

-- NOT EXISTS using a different table: expect id 1 (t3 has ids 2,3).
SELECT 'q9_not_exists_other' AS case_name, t1.id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t3 WHERE t3.id = t1.id) ORDER BY t1.id;

-- NOT IN list with NULL on the left: expect empty result.
SELECT 'q10_not_in_left_null' AS case_name, t1.id FROM t1 WHERE t1.v NOT IN (10, 20) ORDER BY t1.id;

-- IN list with NULL on the left: expect ids 1,2 (NULL excluded).
SELECT 'q11_in_left_null' AS case_name, t1.id FROM t1 WHERE t1.v IN (10, 20) ORDER BY t1.id;

-- NOT IN subquery with NULL filtered out (value column): expect id 2.
SELECT 'q12_not_in_subq_value' AS case_name, t1.id FROM t1 WHERE t1.v NOT IN (SELECT v FROM t2 WHERE v IS NOT NULL) ORDER BY t1.id;
