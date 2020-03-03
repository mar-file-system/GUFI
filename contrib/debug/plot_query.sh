#!/usr/bin/env bash

if [[ "$#" -lt 1 ]]
then
    echo "Syntax: $0 file"
    exit 1
fi

file="$1"

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
sqlsum
sqlent
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

thread_ids=$(awk '{ print $1 }' ${file}.wf | sort -n | uniq)
lowest=$(echo "${thread_ids}" | head -n 1)
highest=$(echo "${thread_ids}" | tail -n 1)

gnuplot <<EOF &

set terminal pngcairo color solid size 12800,4800 font ",32"
set output '${file}.png'
set title "gufi_query Events"
set xlabel "Seconds Since Arbitrary Epoch"
set ylabel "Thread #"
set key outside right
set yrange [${lowest}:${highest}]
set ytics 5

plot '${file}.wf'                using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 16 title 'wf', \
     '${file}.wf_process_work'   using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 8 title 'wf_process_work', \
     '${file}.opendir'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'opendir', \
     '${file}.opendb'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'opendb', \
     '${file}.descend'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'descend', \
     '${file}.sqlsum'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'sqlsum', \
     '${file}.sqlent'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'sqlent', \
     '${file}.closedb'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'closedb', \
     '${file}.closedir'          using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'closedir', \
     '${file}.output_timestamps' using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'output_timestamps', \
     '${file}.wf_next_work'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 8 linecolor 'blue' title 'wf_next_work', \


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
set yrange [${lowest}:${highest}]
set ytics 5

plot '${file}.wf'                using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 16 title 'wf', \
     '${file}.wf_process_work'   using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 8 title 'wf_process_work', \
     '${file}.opendir'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'opendir', \
     '${file}.opendb'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'opendb', \
     '${file}.descend'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'descend', \
     '${file}.sqlsum'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'sqlsum', \
     '${file}.sqlent'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'sqlent', \
     '${file}.closedb'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'closedb', \
     '${file}.closedir'          using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'closedir', \
     '${file}.output_timestamps' using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 2 title 'output_timestamps', \
     '${file}.wf_next_work'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 8 linecolor 'blue' title 'wf_next_work', \

EOF

wait
