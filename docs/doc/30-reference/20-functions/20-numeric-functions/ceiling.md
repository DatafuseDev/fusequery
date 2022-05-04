---
title: CEILING
description: CEILING(x) function
---

Returns the smallest integer value not less than x.

## Syntax

```sql
CEILING(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The numerical value. |

## Return Type

A Float64 data type value.

## Examples

```sql
SELECT CEILING(1.23);
+---------------+
| CEILING(1.23) |
+---------------+
|             2 |
+---------------+

SELECT CEILING(-1.23);
+-------------------+
| CEILING((- 1.23)) |
+-------------------+
|                -1 |
+-------------------+
```
