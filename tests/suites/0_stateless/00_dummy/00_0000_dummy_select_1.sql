SELECT 1;
-- SELECT x; -- {ErrorCode 1058}
SELECT 'a';
SELECT NOT(1=1);
SELECT NOT(1);
SELECT NOT(1=1) from numbers(3);
SELECT TRUE;
SELECT FALSE;
SELECT NOT(TRUE);
SELECT 'That''s good.';
-- SELECT *; -- {ErrorCode 1065}
