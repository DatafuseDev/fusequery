set enable_planner_v2 = 1;

DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a Int8 null, b UInt32 null, c DateTime null, d String null) Engine = Fuse;


INSERT INTO t1 (a,b,c,d) VALUES(-1, 33, '2021-08-15 10:00:00', 'string1234'),
                                       (101, 67, '2021-11-15 10:00:00', 'string5678');

select * from t1;
select sum(a),sum(b) from t1;


DROP DATABASE db1;

set enable_planner_v2 = 0;
