# gufi_query debug prints

## cumulative_times
`gufi_query` provides a series cumulative times values, for example `Real Time (main)`, `sqlsum`, and `sqlent`. For a full list of cumulative_time names, [see first entry in `COLUMN_FORMATS` in cumulative_times.py](cumulative_times.py). The name of these debug values are used for graphing using a config file. [Read about config files here](../../../configs/)

Through commit history cumulative_times values are either added, removed, or their names are changed. For example, `Real time` changed to `Real time (main)`. Any time a cumulative_time value changed, a new set of cumulative_time values was added as a known format to parse. Each set of cumulative_times values gathered is checked against the known column formats.

Below is a table listing commit ranges grouped by format change
* **NOTE**: These ranges are all inclusive [start, end]

|      Commits                       | Number of commits  |  Comments                     |
| :--------------------------------: | :----------------: | :---------------------------- |
| 908c161..HEAD                      |        316+        |                               |
| 61c0a9d..8060d30                   |          4         |                               |
| 7cd35f8..4164985                   |         19         |                               |
| a13a330..216ef5b                   |         51         |                               |
| 75e2c5b..00ba871                   |         13         |                               |
| 093dc32..3235400                   |         74         |                               |
| 941e8ca..97fabf7                   |         64         |                               |
|     aad5b08                        |          1         |                               |
| 90611bf..aaa5b89                   |        107         |                               |
| 86d3d0e..a9a1ef7                   |         18         |                               |
| 831705f..6bde154                   |         18         | SEGFAULTS                     |
| 8480cd1..ea4c148                   |         18         | Same as 86d3d0e..a9a1ef7      |
| 69213c6..1a95766                   |        42          | entries required for -E query |
| FIRST..54bfc53                     |       ????         | Fails to build                |

## Line format

Each Line of the cumulative_times output has the following format:

`<name><separator> <value>`

**Name**
* What is being measured eg: `Real time (main)`
* Each unique field is represented as a column in the database that stores numbers. 

**Separator**
* Character that separates the name from its value
* The current supported separators are a colon (`:`) or a single space. 

**Value**
* Extracted value corresponding to the field
* If time (seconds), if an `s` is present, it is ignored 

**Example**: `Real Time: 32.6s`
