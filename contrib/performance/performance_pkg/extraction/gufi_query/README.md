# gufi_query debug prints

## cumulative_times
One type of debugging information that `gufi_query` outputs is called cumulative_times. Cumulative_times is a collection of mostly time values for key events that occur during `gufi_query`. Some examples include `Real Time (main)`, `sqlsum`, and `sqlent`. For the current cumulative_times format, [see first entry in `COLUMN_FORMATS` in cumulative_times.py](cumulative_times.py). The name of these debug values are used for graphing using a [config file](../../../configs/README.md). 

Through commit history cumulative_time values are either added, removed, or renamed. Any time a cumulative_time value changed, a new set of cumulative_time values was defined as a known format to parse. Each set of cumulative_times values gathered is checked against the known column formats.

Below is a table (possibly not up to date) listing commit ranges grouped by format. To see all available cumulative_time formats as well as the changes introduced in each one, [see the `COLUMN_FORMATS` dictionary in cumulative_times.py](cumulative_times.py)
* **NOTE**: These ranges are all inclusive [start, end]


|      Commits                       | Number of commits  |  Comments                                                                                 |
| :--------------------------------: | :----------------: | :---------------------------------------------------------------------------------------: |
| 908c161..HEAD                      |        313+        |                                                                                           |
| 61c0a9d..8060d30                   |          5         |                                                                                           |
| 7cd35f8..4164985                   |         20         |                                                                                           |
| a13a330..216ef5b                   |         52         | Last occurrence of segfault bug ([See notes](#missed-null-check))  |
| 75e2c5b..00ba871                   |         14         |                                                                                           |
| 093dc32..3235400                   |         75         |                                                                                           |
| 941e8ca..97fabf7                   |         65         |                                                                                           |
|     aad5b08                        |          1         |                                                                                           |
| 90611bf..aaa5b89                   |        108         | First occurrence of segfault bug ([See notes](#missed-null-check)) |
| 5ec5386..a9a1ef7                   |         51         | Bugged commit ranges, Performance increase ([See notes](#8480cd1a9a1ef7))                 |
| 2111dd1..7172479                   |        509         | UNDOCUMENTED                                                                              |

## Line format
Each line of the cumulative_times output has the following format:

`<name><separator> <value>`

* **Name**
    * What is being measured eg: `Real time (main)`
    * Each name has a corresponding column in the database
        * Including whitespace, so column names must be referred to with quotation marks

* **Separator**
    * Character that separates the name from its values
    * The current supported separators are a colon (`:`) or a single space

* **Value**
    * Extracted time value corresponding to the field
    * If an `s` is present at the end of the value, it is ignored

**Example**: `Real Time: 32.6s`

## Notes

### Ranges

#### 8480cd1..a9a1ef7

* Sub range `86d3d0e..a9a1ef7`
    * Functions normally

* Sub range `831705f..6bde154`
    * Thread count must be <= 48 ([See `Thread count > 48 segfaults`](#thread-count--48-segfaults))
    * [`831705f`](https://github.com/mar-file-system/GUFI/commit/831705f) causes a significant increase in performance by reducing the amount of memory being zeroed

* Sub range `8480cd1..ea4c148`
    * Functions normally

* Sub range `5ec5386..1a95766`
    * `entries` table required for -E query ([See `Aggregation Error`](#aggregation-error))

### Known bugs

#### Missed NULL Check

* In the commit range `b393254..216ef5b` (254 total commits), there exists a bug in which `NULL` values are not handled correctly. If a selected column contains a value that sqlite passes to the print callback as `NULL`, `gufi_query` will segfault.

#### Thread count > 48 segfaults
* In the commit range `831705f..6bde154` (3 commits) there is a hardcoded value of 48 that prevents `gufi_query` from being run at thread counts greater than 48.

* If you need to run in this commit range with a higher thread count, change the 48 in `struct descend_timers global_timers[48];` in [gufi_query.c](https://github.com/mar-file-system/GUFI/blob/831705f1e0d69e4f322c4108e4bf512fbaebda9b/src/gufi_query.c#L165) to the number of threads you wish to use.

#### Aggregation Error
* In the commit range `5ec5386..1a95766` (20 commits, possibly even further back), trying to use the pentries table will produce the message `failed to create result aggregation database file:aggregate-1?mode=memory&cache=shared: no such table: pentries:`.

* The pentries table **does** exist at this point in commit history. The issue is due to errors in the aggregation code.