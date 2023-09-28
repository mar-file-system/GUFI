#! /bin/env bash

idx=/mnt/nvme3n1/jbent/jbent_home/
nthreads=224

# simple array so we can do various queries on both files and dirs
declare -A my_types=( ["dir"]="vrsummary" ["file"]="vrpentries" )

# find largest directory and largest file
for Type in "${!my_types[@]}"; do
  Table="${my_types[$Type]}"
  echo "Size of largest $Type"
  gufi_query \
    -I "CREATE TABLE intermediate(size INT64);" \
    -S "INSERT INTO intermediate SELECT max(size) FROM $Table;" \
    -K "CREATE TABLE aggregate(size INT64);" \
    -J "INSERT INTO aggregate SELECT max(size) FROM intermediate;" \
    -G "SELECT max(size) FROM aggregate;" \
    -n $nthreads \
    $idx
done

# some statistics about the directories 
for Type in "${!my_types[@]}"; do
  Table="${my_types[$Type]}"
  echo "Some $Type size aggregates"
  gufi_query \
    -I "CREATE TABLE intermediate(size INT64);" \
    -S "INSERT INTO intermediate SELECT size FROM $Table;" \
    -K "CREATE TABLE aggregate(size INT64);" \
    -J "INSERT INTO aggregate SELECT size FROM intermediate;" \
    -G "SELECT '$Type max is ', max(size), ', min is ', min(size), ', avg is ', avg(size) FROM aggregate ORDER BY size DESC;" \
    -n $nthreads \
    $idx
done

# get a count of zero length files
echo "Zero length files"
gufi_query \
  -I "CREATE TABLE intermediate(quantity INT64);" \
  -S "INSERT INTO intermediate SELECT count(*) FROM vrpentries where size=0;" \
  -K "CREATE TABLE aggregate(quantity INT64);" \
  -J "INSERT INTO aggregate SELECT count(*) FROM intermediate;" \
  -G "SELECT sum(quantity) FROM aggregate;" \
  -n $nthreads \
  $idx

# now let's query the vrsummary which already has aggregated info about file sizes to create some bins by file size
gufi_query \
  -I "CREATE TABLE intermediate(totltk INT64,totmtk INT64,totltm INT64,totmtm INT64,totmtg INT64,totmtt INT64);" \
  -S "INSERT INTO intermediate SELECT totltk,totmtk,totltm,totmtm,totmtg,totmtt from vrsummary;" \
  -K "CREATE TABLE aggregate(totltk INT64,totmtk INT64,totltm INT64,totmtm INT64,totmtg INT64,totmtt INT64);" \
  -J "INSERT INTO aggregate SELECT totltk,totmtk,totltm,totmtm,totmtg,totmtt from intermediate;" \
  -G "SELECT 'totltk is ', sum(totltk), ' , totmtk is ', sum(totmtk), ' , totltm is ', sum(totltm), ' , totmtm is ', sum(totmtm), ' , totmtg is ', sum(totmtg), ' , totmtt is ', sum(totmtt) \
      FROM aggregate;" \
  -n $nthreads \
  $idx


