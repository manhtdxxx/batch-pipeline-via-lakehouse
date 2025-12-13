-- Create schema
CREATE SCHEMA IF NOT EXISTS iceberg.demo_db;

-- Drop table
DROP TABLE IF EXISTS iceberg.demo_db.sales;

-- Create table
CREATE TABLE iceberg.demo_db.sales (
    id INT,
    product VARCHAR,
    amount DOUBLE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['product']
);

-- Commit 1
INSERT INTO iceberg.demo_db.sales (id, product, amount) VALUES
(1, 'Apple', 10.5),
(2, 'Banana', 5.0),
(3, 'Orange', 7.25);

-- Commit 2
INSERT INTO iceberg.demo_db.sales (id, product, amount) VALUES
(4, 'Apple', 12.0),
(5, 'Banana', 6.0),
(6, 'Orange', 8.5);

-- Commit 3
INSERT INTO iceberg.demo_db.sales (id, product, amount) VALUES
(7, 'Apple', 11.0),
(8, 'Banana', 5.5),
(9, 'Orange', 7.75);

-- Select data from the latest snapshot
SELECT * FROM iceberg.demo_db.sales;

-- View the snapshot history of the table
SELECT * FROM iceberg.demo_db."sales$snapshots";

-- Select data from a specific snapshot
SELECT * FROM iceberg.demo_db.sales FOR VERSION AS OF 7788023717590674144;
