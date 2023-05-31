# Config files

Config files are used to define a graph's parameters. Each config file is divided up into sections based on how they affect the graph. The `configparser` Python library is used to parse these files.

## Sections and Keys

Sections and Keys may change without an immediate adjustment to this readme, to see an exhaustive list of all available sections and keys, see [config.py](../performance_pkg/graph/config.py)

* Note: All lists are comma seperated

### [raw_data]

#### commits
* Description: Commit hashes to plot
    * **NOTE**: A commit hash will not be plotted if it has no debug value data unless [`x_full_range`](#x_full_range) in the `axes` section is set to `True`
* Data Type: List of commit hashes (strings)
    * Can be any [commit-ish or tree-ish identifier](https://stackoverflow.com/a/23303550) that `git` can convert into commit hashes:
    * Example: `b34561a, ce0751d..c8cdd6e`

#### columns
* Description: Column names from a set of debug values stored in a raw data database. See [extraction](../performance_pkg/extraction/) folder and search the GUFI command subdirectories for names
* Data Type: List of columns (strings)
    * Example: `Real Time (main)`

### [output]

#### path
* Description: Path to save generated graph
* Data Type: string
    * Example: `~/graph.png`

#### graph_title
* Description: Title displayed at the top of the graph
* Data Type: string
    * Example: `My Graph`

#### graph_dimensions
* Description: Dimensions of graph in inches
* Data Type: `<width>`,`<height>` (**positive_float**,**positive_float**)
    * Examples: `12,6`
    * See [Matplotlib: Figure size](https://matplotlib.org/stable/gallery/subplots_axes_and_figures/figure_size_units.html)

### [lines]

**NOTE**: For all line keys, the default cycler will engage if not enough entries are provided to match the number of columns
* See [Matplotlib: Styling with cycler](https://matplotlib.org/stable/tutorials/intermediate/color_cycle.html)

#### colors
* Description: Color corresponding to a column's line
* Data Type: List of strings
    * Examples: `blue, green`
    * See [Matplotlib: Specifying colors](https://matplotlib.org/stable/tutorials/colors/colors.html) for a full list of supported color values (Excluding tuples)

#### types
Description: Line type/style corresponding to a column's line
* Data Type: List of line types/styles (strings)
    * Examples: `solid, dashed`
    * See [Matplotlib: Linestyles](https://matplotlib.org/stable/gallery/lines_bars_and_markers/linestyles.html) for a full list of supported line types and styles

#### markers
* Description: Line markers corresponding to a column's line
* Data Type: List of markers (strings)
    * Examples: `o,^`
    * See [Matplotlib: Markers](https://matplotlib.org/stable/api/markers_api.html) for a full list of supported markers

### [axes]

#### hash_len
* Description: Length of hash written out for each x-tick
* Data Type: int
    * Examples: `6`
        * positive number = characters from beginning
        * negative number = characters from end
        * 0 = full hash

#### x_label_size
* Description: Font size of x-labels beneath each x-tick
* Data Type: Keyword or number (string)
    * Examples: `10`
        * Can be a float value
        * Can be a descriptor (Small, Large)

#### x_label_rotation
* Description: Rotation of x-labels in degrees beneath each x-tick
* Data Type: float
    * Examples: `45`
        * Positive: Slant upwards
        * Negative: Slant downwards

#### x_full_range
* Description: Plot all commits in the defined range, even if no data is present
* Data Type: boolean
    * Example `True`

#### x_reorder
* Description: Whether or not to reorder identifiers in `commits` to match git history order
* Data Type: boolean
    * Example `True`

#### y_label
* Description: Label of the y axis
* Data Type: string
    * Example: `Time (s)`

#### y_stat
* Description: Statistic to plot for each column
* Data Type: string
    * Example: `average`
        * To see all available check the [stats.py](../performance_pkg/graph/stats.py)

#### y_min
* Description: Minimum value on the y-axis
* Data Type: float
    * Example: `0`
        * If min is > y_max, the axis will flip

#### y_max
* Description: Maximum value on the y-axis
* Data Type: float
    * Example: `30`
        * If max is < y_min, the axis will flip

**NOTE**: If y_min and y_max are both provided no values, matplotlib will use a default range based on the data

#### annotate
* Description: Annotate y_stat values next to their respective points
* Data Type: boolean
    * Example: `True`

### [error_bar]

#### bottom
* Description: Statistic to be represented as the bottom of the errorbar
* Data Type: String
    * Example: `minimum`
        * To see all available statistics, see [stats.py](../performance_pkg/graph/stats.py)

#### top
* Description: Statistic to be represented as the top of the errorbar
* Data Type: String
    * Example: `maximum`
        * To see all available statistics, see [stats.py](../performance_pkg/graph/stats.py)

#### colors
* Description: Color corresponding to a column's line
* Data Type: List of colors (strings)
    * Examples: `blue, green`
    * See [Matplotlib: Specifying colors](https://matplotlib.org/stable/tutorials/colors/colors.html) for a full list of supported color values (Excluding tuples)
* **NOTE**: The default cycler will engage if not enough entries are provided to match the number of columns

#### capsize
* Description: The length of the errorbar caps in points (points = pixels by default)
* Data Type: positive float
    * Example: `10`
    * See [Matplotlib: Errorbar](https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.errorbar.html) and view the capsize section

#### annotate
* Description: Annotate statistic values next to their respective errorbar height
* Data Type: boolean
    * Example: `True`

### [annotations]

#### precision
* Description: How many decimal places out to display in annotations
* Data type: positive int
    * Example: `2`

#### x_offset
* Description: x offset value from point to write annotation
* Data Type: float
    * Example: `5`

#### y_offset
* Description: y offset value from point to write annotation
* Data Type: float
    * Example: `5`

#### text_colors
* Description: List of text color annotations corresponding to a column's line
* Data Type: List of colors (strings)
    * Examples: `blue, green`
    * See [Matplotlib: Specifying colors](https://matplotlib.org/stable/tutorials/colors/colors.html) for a full list of supported color values (Excluding tuples)

## Overrides

Below is a full list of all available config key overrides when calling **graph_performance.py**:

### [raw_data]
* --raw_data_commits
* --raw_data_columns

### [output]
* --output_path
* --output_graph_title
* --output_graph_dimensions

### [lines]
* --lines_colors
* --lines_types
* --lines_markers

### [axes]
* --axes_hash_len
* --axes_x_label_size
* --axes_x_label_rotation
* --axes_x_full_range
* --axes_x_reorder
* --axes_y_label
* --axes_y_stat
* --axes_y_min
* --axes_y_max
* --axes_annotate

### [error_bar]
* --error_bar_bottom
* --error_bar_top
* --error_bar_colors
* --error_bar_cap_size
* --error_bar_annotate

### [annotations]
* --annotations_precision
* --annotations_x_offset
* --annotations_y_offset
* --annotations_text_colors
