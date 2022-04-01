SELECT REGEXP_INSTR('dog cat dog', 'dog');
SELECT REGEXP_INSTR('dog cat dog', 'dog', 2);
SELECT REGEXP_INSTR('dog cat dog', 'dog', 1, 2);
SELECT REGEXP_INSTR('dog cat dog', 'dog', 1, 2, 1);
SELECT REGEXP_INSTR('dog cat dog', 'DOG', 1, 2, 1);
SELECT REGEXP_INSTR('dog cat dog', 'DOG', 1, 2, 1, 'c');
SELECT REGEXP_INSTR('dog cat dog', NULL);
SELECT REGEXP_INSTR('dog cat dog', 'dog', NULL);
--
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(s String NULL, pat String NULL, pos Int64 NULL, occu Int64 NULL, ro Int64 NULL, mt String NULL) Engine = Memory;
INSERT INTO t1 (s, pat, pos, occu, ro, mt) VALUES (NULL, 'dog', 1, 1, 1, ''), ('dog cat dog', 'dog', NULL, 1, 1, 'c'), ('dog cat dog', 'dog', 1, 1, 1, 'c'), ('dog cat dog', 'dog', 1, 1, 1, NULL);
select s from t1 where regexp_instr(s, pat, pos, occu, ro, mt) = 4;
