#!/bin/bash
LOG_DIR=../
function generate_statistics {
    for d in ${LOG_DIR}/pegasus_*/ ; do
        echo "$d"
        pegasus-statistics -s all ${d}
    done
}

function pegasus_runtime {
    for d in ${LOG_DIR}/pegasus_*/ ; do
        individuals_time=$(cat ${d}/statistics/breakdown.txt | grep "\<individuals\>" | grep -m1 successful | awk -F' ' '{print $5}')
        merge_time=$(cat ${d}/statistics/breakdown.txt | grep "\<individuals_merge\>" | grep -m1 successful | awk -F' ' '{print $5}') 
        runtime=$(bc <<<"$merge_time + $individuals_time")
        echo "$d ${runtime}"
    done
}

function decaf_runtime {
    for d in ${LOG_DIR}/decaf_*/ ; do
        ctime=$(cat ${d}/00/00/merge_cluster1.out | tail -1 | sed 's/.*: //')
        echo "$d ${ctime}"
    done
}

function get_stats {
    # cat stat | grep $1 | awk -F' ' '{print $2}'
    cat $1 | grep $2 | awk '{if($2!=""){count++;sum+=$2};y+=$2^2} END{sq=sqrt(y/NR-(sum/NR)^2);sq=sq?sq:0;print "mean = "sum/count ORS "std = ",sq}'
}

stat_file="stat"
pegasus_runtime > ${stat_file}
decaf_runtime >> ${stat_file}
opts=( "pegasus" "decaf" )
sizes=( 5 10 16 )
for opt in "${opts[@]}"; do
    for size in "${sizes[@]}"; do
        echo ${opt}_${size}
        get_stats ${stat_file} ${opt}_${size}
    done
done