CREATE TABLE math_sample_numbers (timestamp UInt32, value Int32) engine=Memory;
INSERT INTO math_sample_numbers VALUES ('1', '-1'), ('2', '-2'), ('3', '3');

SELECT pi();
SELECT abs(-1);
SELECT abs(-10086);
SELECT abs('-233.0');
SELECT abs('blah');
SELECT abs(TRUE); -- {ErrorCode 7}
SELECT abs(NULL); -- {ErrorCode 7}
SELECT abs(value) FROM math_sample_numbers;
SELECT abs(value) + abs(-1) FROM math_sample_numbers;
-- TODO: log(NULL) should returns NULL
SELECT log(NULL);
SELECT log(NULL, NULL); -- {ErrorCode 10}
SELECT log(1, NULL);
SELECT log(NULL, 1); -- {ErrorCode 10}
SELECT log('10', 100);

DROP TABLE math_sample_numbers;
