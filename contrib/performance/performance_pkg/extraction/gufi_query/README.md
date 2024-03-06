# gufi_query debug prints

## cumulative_times
One type of debugging information that `gufi_query` outputs is called cumulative_times. Cumulative_times is a collection of mostly time values for key events that occur during `gufi_query`. Some examples include `Real Time (main)`, `sqlsum`, and `sqlent`. For the current cumulative_times format, [see first entry in `COLUMN_FORMATS` in cumulative_times.py](cumulative_times.py). The name of these debug values are used for graphing using a [config file](../../../configs/README.md).

Through commit history cumulative_time values are either added, removed, or renamed. Any time a cumulative_time value changed, a new set of cumulative_time values was defined as a known format to parse. Each set of cumulative_times values gathered is checked against the known column formats.

Below is a table (possibly not up to date) listing commit ranges grouped by format. To see all available cumulative_time formats as well as the changes introduced in each one, [see the `COLUMN_FORMATS` dictionary in cumulative_times.py](cumulative_times.py)
* **NOTE**: These ranges are all inclusive [start, end]


|      Commits   | Number of commits  |  Comments  |
| :------------: | :----------------: | :--------: |
| [c42ee8a](https://github.com/mar-file-system/GUFI/commit/c42ee8a), [HEAD](https://github.com/mar-file-system/GUFI/commit/HEAD) | 2+ | |
| [908c161](https://github.com/mar-file-system/GUFI/commit/908c161), [d743512](https://github.com/mar-file-system/GUFI/commit/d743512) | 681 | |
| [61c0a9d](https://github.com/mar-file-system/GUFI/commit/61c0a9d), [8060d30](https://github.com/mar-file-system/GUFI/commit/8060d30)| 5 | |
| [7cd35f8](https://github.com/mar-file-system/GUFI/commit/7cd35f8), [4164985](https://github.com/mar-file-system/GUFI/commit/4164985)| 20 | |
| [a13a330](https://github.com/mar-file-system/GUFI/commit/a13a330), [216ef5b](https://github.com/mar-file-system/GUFI/commit/216ef5b)| 52 | Performance increase, Last occurrence of Missed NULL bug ([See notes](#a13a330-216ef5b)) |
| [75e2c5b](https://github.com/mar-file-system/GUFI/commit/75e2c5b), [00ba871](https://github.com/mar-file-system/GUFI/commit/00ba871)| 14 | Performance decrease ([See notes](#75e2c5b-00ba871))|
| [093dc32](https://github.com/mar-file-system/GUFI/commit/093dc32), [3235400](https://github.com/mar-file-system/GUFI/commit/3235400)| 75 | |
| [941e8ca](https://github.com/mar-file-system/GUFI/commit/941e8ca), [97fabf7](https://github.com/mar-file-system/GUFI/commit/97fabf7)| 65 | |
| [aad5b08](https://github.com/mar-file-system/GUFI/commit/aad5b08)| 1 | |
| [90611bf](https://github.com/mar-file-system/GUFI/commit/90611bf), [aaa5b89](https://github.com/mar-file-system/GUFI/commit/aaa5b89)| 108 | Performance increase, First occurrence of Missed NULL bug ([See notes](#90611bf-aaa5b89)) |
| [5ec5386](https://github.com/mar-file-system/GUFI/commit/5ec5386), [a9a1ef7](https://github.com/mar-file-system/GUFI/commit/a9a1ef7)| 51 | Bugged commit ranges, Performance increase ([See notes](#5ec5386-a9a1ef7)) |
| [7172479](https://github.com/mar-file-system/GUFI/commit/7172479) | 1 | |
| [7acefa5](https://github.com/mar-file-system/GUFI/commit/7acefa5), [466e9e8](https://github.com/mar-file-system/GUFI/commit/466e9e8)| 8 | |
| [54bfc53](https://github.com/mar-file-system/GUFI/commit/54bfc53), [90b21bc](https://github.com/mar-file-system/GUFI/commit/90b21bc)| 15 | 54bfc53 contains multiple bugs ([See notes](#54bfc53-90b21bc))|
| [4823587](https://github.com/mar-file-system/GUFI/commit/4823587), [a6631c4](https://github.com/mar-file-system/GUFI/commit/a6631c4)| 18 | Bugged commits and ranges ([See notes](#4823587-a6631c4))|
| [046f533](https://github.com/mar-file-system/GUFI/commit/046f533), [b8c156c](https://github.com/mar-file-system/GUFI/commit/b8c156c)| 4 | Bugged commits and ranges ([See notes](#046f533-b8c156c))|
| [ce61cb7](https://github.com/mar-file-system/GUFI/commit/ce61cb7), [f5e755a](https://github.com/mar-file-system/GUFI/commit/f5e755a)| 12 | Performance increase ([See notes](#cf62851-f5e755a))|
| [2111dd1](https://github.com/mar-file-system/GUFI/commit/2111dd1), [cf62851](https://github.com/mar-file-system/GUFI/commit/cf62851)| 451 | gufi_query does not exist; still called bfq |

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

#### a13a330, 216ef5b
* On commit [`216ef5b`](https://github.com/mar-file-system/GUFI/commit/216ef5b)
    * Last occurrence of [`Missed NULL Check`](#missed-null-check)

* On commit [`557aa5e`](https://github.com/mar-file-system/GUFI/commit/557aa5e)
    * Performance increase likely caused by the changes made to the `print_callback` function

#### 75e2c5b, 00ba871
* On commit [`b4bb77d`](https://github.com/mar-file-system/GUFI/commit/b4bb77d)
    * Performance reduction likely caused by the ordering being done in the `print_callback` function

#### 90611bf, aaa5b89
* On commit [`b393254`](https://github.com/mar-file-system/GUFI/commit/b393254)
    * First ocuurence of [`Missed NULL Check`](#missed-null-check)

* On commit [`d74a2cc`](https://github.com/mar-file-system/GUFI/commit/d74a2cc)
    * Performance increase caused by changes in the `print_callback` function

#### 5ec5386, a9a1ef7
* Sub range `86d3d0e, a9a1ef7`
    * Functions normally

* Sub range `831705f, 6bde154`
    * Thread count must be <= 48 ([See `Thread count > 48 segfaults`](#thread-count--48-segfaults))
    * [`831705f`](https://github.com/mar-file-system/GUFI/commit/831705f) causes a significant increase in performance by reducing the amount of memory being zeroed

* Sub range `8480cd1, ea4c148`
    * Functions normally

* Sub range `5ec5386, 1a95766`
    * `entries` table required for -E query ([See `Aggregation Error`](#aggregation-error))

* On commit [`d6ec34f`](https://github.com/mar-file-system/GUFI/commit/d6ec34f)
    * Last instance of [`Epoch Error`](#multiple-definitions-of-epoch)

#### 54bfc53, 90b21bc
* On commit [`54bfc53`](https://github.com/mar-file-system/GUFI/commit/54bfc53)
    * Last instance of [`Missing fill_queues.c`](#missing-fill_queuesc)
    * First instance of [`Multiple definitions of epoch`](#multiple-definitions-of-epoch)

#### 4823587, a6631c4
* On commit [`a6631c4`](https://github.com/mar-file-system/GUFI/commit/a6631c4)
    * First instance of [`Missing fill_queues.c`](#missing-fill_queuesc)
* On commit [`dd9d284`](https://github.com/mar-file-system/GUFI/commit/dd9d284)
    * Debug mode does not work, cumulative times debug values not printed.
* Sub range `977bc6b, 64a511d`
    * Functions normally
* On commit [`428398f`](https://github.com/mar-file-system/GUFI/commit/428398f)
    * Last instance of [`Errno bug`](#Errno-bug)

#### 046f533, b8c156c
* On commit [`a4c5fd1`](https://github.com/mar-file-system/GUFI/commit/a4c5fd1)
    * First instance of [`Errno bug`](#Errno-bug)

#### cf62851, f5e755a
* On commit [`f5e755a`](https://github.com/mar-file-system/GUFI/commit/f5e755a)
    * Performance increase caused by switching to QPTPool, reducing contention

### Known bugs

#### Missed NULL Check
* In commit range `b393254, 216ef5b` (254 total commits), there exists a bug in which `NULL` values are not handled correctly. If a selected column contains a value that sqlite passes to the print callback as `NULL`, `gufi_query` will segfault.

* Fixed on [`7cd35f8`](https://github.com/mar-file-system/GUFI/commit/7cd35f8)

#### Thread count > 48 segfaults
* In commit range `831705f, 6bde154` (3 commits) there is a hardcoded value of 48 that prevents `gufi_query` from being run at thread counts greater than 48.

* If you need to run in this commit range with a higher thread count, change the 48 in `struct descend_timers global_timers[48];` in [gufi_query.c](https://github.com/mar-file-system/GUFI/blob/831705f1e0d69e4f322c4108e4bf512fbaebda9b/src/gufi_query.c#L165) to the number of threads you wish to use.

* Fixed on [`86d3d0e`](https://github.com/mar-file-system/GUFI/commit/86d3d0e)

#### Aggregation Error
* In commit range `5ec5386, 1a95766` (20 commits, possibly even further back), trying to use the pentries table will produce the message `failed to create result aggregation database file:aggregate-1?mode=memory&cache=shared: no such table: pentries:`.

* The pentries table **does** exist at this point in commit history. The issue is due to errors in the aggregation code.

* Fixed on [`8480cd1`](https://github.com/mar-file-system/GUFI/commit/8480cd1)

#### Multiple definitions of epoch
* In commit range `54bfc53, d6ec34f` (33 commits) epoch is defined multiple times.

* Some versions of gcc such as 7.3.1 from `devtoolset-7` (CentOS 7 SCL) miss this bug which means it is possible to build these commits.

* Fixed on [`831fe76`](https://github.com/mar-file-system/GUFI/commit/831fe76)

#### Missing fill_queues.c
* On commit `a6631c4`, `fillqueues.c` is referenced in contrib/CMakeLists.txt, but this file does not exist.

* This bug occurs only on commits `a6631c4` and `54bfc53`.

* Fixed on [`69213c6`](https://github.com/mar-file-system/GUFI/commit/69213c6)

#### Errno bug
* In commit range `a4c5fd1, 428398f` (10 commits) errno is not included in `gufi_dir2trace.c` despite being referenced. Will not build.

* Fixed on [`7202174`](https://github.com/mar-file-system/GUFI/commit/7202174)