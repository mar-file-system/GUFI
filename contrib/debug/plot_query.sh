#!/usr/bin/env bash

if [[ "$#" -lt 2 ]]
then
    echo "Syntax: $0 threads file"
    exit 1
fi

threads="$1"
file="$2"

names="
addqueryfuncs
attach
check_args
clone
closedb
closedir
create_tables
descend
detach
isdir
isdir_branch
level
level_branch
load_extensions
opendb
opendir
output_timestamps
pushdir
readdir
readdir_branch
set
set_pragmas
snprintf
sqlite3_exec
sqlite3_open_v2
strncmp
strncmp_branch
utime
wf
wf_broadcast
wf_cleanup
wf_ctx_mutex_lock
wf_get_queue_head
wf_move
wf_next_work
wf_process_queue
wf_process_work
wf_sll_init
wf_tw_mutex_lock
wf_wait
while_branch
within_descend
"

for name in ${names}
do
    grep -E "[0-9]+ ${name} [0-9]+ [0-9]+" "${file}" > "${file}.${name}" &
done

wait

gnuplot <<EOF &

set terminal pngcairo color solid size 6400,3200 font ",32"
set output '${file}.png'
set title "gufi_query Events"
set xlabel "Seconds Since Arbitrary Epoch"
set ylabel "Thread #"
set key outside right
set yrange [-1:${threads}]
set ytics 5

plot '${file}.wf'                using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 4 title 'wf', \
     '${file}.wf_process_work'   using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'wf_process_work', \
     '${file}.opendir'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'opendir', \
     '${file}.opendb'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'opendb', \
     '${file}.descend'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'descend', \
     '${file}.sqlite3_exec'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'sqlite3_exec', \
     '${file}.closedb'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'closedb', \
     '${file}.closedir'          using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'closedir', \
     '${file}.output_timestamps' using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'output_timestamps', \
     '${file}.wf_next_work'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 linecolor 'blue' title 'wf_next_work', \

EOF

gnuplot <<EOF &

set terminal pngcairo color solid size 6400,3200 font ",31"
set output '${file}-zoomed-in.png'
set title "gufi_query Events"
set xlabel "Seconds Since Arbitrary Epoch"
set ylabel "Thread #"
set clip two
set key outside right
set xrange [1.1:1.15]
set yrange [-1:${threads}]
set ytics 5

plot '${file}.wf'                using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 4 title 'wf', \
     '${file}.wf_process_work'   using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'wf_process_work', \
     '${file}.opendir'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'opendir', \
     '${file}.opendb'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'opendb', \
     '${file}.descend'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'descend', \
     '${file}.sqlite3_exec'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'sqlite3_exec', \
     '${file}.closedb'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'closedb', \
     '${file}.closedir'          using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'closedir', \
     '${file}.output_timestamps' using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 1 title 'output_timestamps', \
     '${file}.wf_next_work'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 linecolor 'blue' title 'wf_next_work', \

EOF

wait
