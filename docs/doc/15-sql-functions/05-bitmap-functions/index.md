---
title: Bitmap Functions
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.2.26"/>

| Function                                   	| Description                                                                                                  	| Example                                                            	| Result    	|
|--------------------------------------------	|--------------------------------------------------------------------------------------------------------------	|--------------------------------------------------------------------	|-----------	|
| bitmap_count(bitmap)                       	| Counts the number of bits set to 1 in the bitmap.                                                            	| bitmap_count(build_bitmap([1,4,5]))                                	| 3         	|
| bitmap_contains(bitmap, value)             	| Checks if the bitmap contains a specific value.                                                              	| bitmap_contains(build_bitmap([1,4,5]), 1)                          	| 1         	|
| bitmap_has_all(bitmap1, bitmap2)           	| Checks if the first bitmap contains all the bits in the second bitmap.                                       	| bitmap_has_all(build_bitmap([1,4,5]), build_bitmap([1,2]))         	| 0         	|
| bitmap_has_any(bitmap1, bitmap2)           	| Checks if the first bitmap has any bit matching the bits in the second bitmap.                               	| bitmap_has_any(build_bitmap([1,4,5]), build_bitmap([1,2]))         	| 1         	|
| bitmap_max(bitmap)                         	| Gets the maximum value in the bitmap.                                                                        	| bitmap_max(build_bitmap([1,4,5]))                                  	| 5         	|
| bitmap_min(bitmap)                         	| Gets the minimum value in the bitmap.                                                                        	| bitmap_min(build_bitmap([1,4,5]))                                  	| 1         	|
| bitmap_or(bitmap1, bitmap2)                	| Performs a bitwise OR operation on the two bitmaps.                                                          	| bitmap_or(build_bitmap([1,4,5]), build_bitmap([6,7]))::String      	| 1,4,5,6,7 	|
| bitmap_and(bitmap1, bitmap2)               	| Performs a bitwise AND operation on the two bitmaps.                                                         	| bitmap_and(build_bitmap([1,4,5]), build_bitmap([4,5]))::String     	| 4,5       	|
| bitmap_xor(bitmap1, bitmap2)               	| Performs a bitwise XOR (exclusive OR) operation on the two bitmaps.                                          	| bitmap_xor(build_bitmap([1,4,5]), build_bitmap([5,6,7]))::String   	| 1,4,6,7   	|
| bitmap_not(bitmap1, bitmap2)               	| Alias for "bitmap_and_not(bitmap1, bitmap2)".                            	| bitmap_count(bitmap_not(build_bitmap([2,3,9]), build_bitmap([2,3,5])))     	| 1  	|
| bitmap_intersect(bitmap)                      | Counts the number of bits set to 1 in the bitmap by performing a logical INTERSECT operation.                 | bitmap_intersect(to_bitmap('1, 3, 5'))::String                     | 1, 3, 5        	|
| bitmap_union(bitmap)                          | Counts the number of bits set to 1 in the bitmap by performing a logical UNION operation.                     | bitmap_union(to_bitmap('1, 3, 5'))::String                     | 1, 3, 5        	|
| bitmap_and_not(bitmap1, bitmap2)           	| Generates a new bitmap with elements from the first bitmap (bitmap1) that are not in the second (bitmap2).                                                     	| bitmap_count(bitmap_and_not(build_bitmap([2,3,9]), build_bitmap([2,3,5]))) 	| 1   	|
| bitmap_subset_limit(bitmap, start, limit)  	| Generates a sub-bitmap of the source bitmap, beginning with a range from the start value, with a size limit. 	| bitmap_subset_limit(build_bitmap([1,4,5]), 2, 2)::String           	| 4,5       	|
| bitmap_subset_in_range(bitmap, start, end) 	| Generates a sub-bitmap of the source bitmap within a specified range.                                        	| bitmap_subset_in_range(build_bitmap([5,7,9]), 6, 9)::String        	| 7         	|
| sub_bitmap(bitmap, start, size)            	| Generates a sub-bitmap of the source bitmap, beginning from the start index, with a specified size.          	| sub_bitmap(build_bitmap([1, 2, 3, 4, 5]), 1, 3)::String            	| 2,3,4     	|
| bitmap_and_count(bitmap)                   	| Counts the number of bits set to 1 in the bitmap by performing a logical AND operation.                      	| bitmap_and_count(to_bitmap('1, 3, 5'))                             	| 3         	|
| bitmap_not_count(bitmap)                      | Counts the number of bits set to 0 in the bitmap by performing a logical NOT operation.                       | bitmap_not_count(to_bitmap('1, 3, 5'))                                |  3            |
| bitmap_or_count(bitmap)                    	| Counts the number of bits set to 1 in the bitmap by performing a logical OR operation.                       	| bitmap_or_count(to_bitmap('1, 3, 5'))                              	| 3         	|
| bitmap_xor_count(bitmap)                   	| Counts the number of bits set to 1 in the bitmap by performing a logical XOR (exclusive OR) operation.       	| bitmap_xor_count(to_bitmap('1, 3, 5'))                             	| 3         	|
| intersect_count('bitmap_value1', 'bitmap_value2')(bitmap_column1, bitmap_column2) | Counts the number of intersecting bits between two bitmap columns.   | intersect_count('a', 'c')(v, tag) from agg_bitmap_test | 1 |