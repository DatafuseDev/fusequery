set enable_planner_v2 = 0;
DROP TABLE IF EXISTS t;

CREATE TABLE t(a int null, b int null);

insert into t values(1,2);

explain select count(*) from t;
explain select 1 from t;
explain select 1 + 1 from t;
explain select now() from t;
explain select sum(a) from t;


DROP TABLE IF EXISTS t;
