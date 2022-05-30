select '=== Test limit ===';
select number from numbers(100) order by number asc limit 10;
select '==================';
select number from numbers(100) order by number asc limit 10;
select '=== Test limit n, m ===';
select number from numbers(100) order by number asc limit 10, 10;
select '==================';
select number from numbers(100) order by number asc limit 10, 10;
select '=== Test limit with offset ===';
select number from numbers(100) order by number asc limit 10 offset 10;
select '==============================';
select number from numbers(100) order by number asc limit 10 offset 10;
select '=== Test offset ===';
select number from numbers(10) order by number asc offset 5;
select '===================';
select number from numbers(10) order by number asc offset 5;
select number from numbers(10000) order by number limit 1;
