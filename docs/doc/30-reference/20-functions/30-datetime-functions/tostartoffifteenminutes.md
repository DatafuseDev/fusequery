---
title: toStartOfFifteenMinutes
---

Rounds down the date with time to the start of the fifteen-minute interval.
## Syntax

```sql
toStartOfFifteenMinutes( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | datetime |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
SELECT toStartOfFifteenMinutes(now());
+--------------------------------+
| toStartOfFifteenMinutes(now()) |
+--------------------------------+
| 2022-03-29 06:45:00            |
+--------------------------------+

SELECT toStartOfFifteenMinutes(to_datetime(1630812366));
+-------------------------------------------------+
| toStartOfFifteenMinutes(to_datetime(1630812366)) |
+-------------------------------------------------+
| 2021-09-05 03:15:00                             |
+-------------------------------------------------+
```
