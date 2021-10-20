#!/bin/bash
LOG_DIR=./log/
stat_file="summary.csv"
function generate_pegasus_statistics {
    for d in ${LOG_DIR}/pegasus_*/ ; do
        echo "$d"
        pegasus-statistics -s all ${d}
    done
}

function pegasus_runtime {
    d=$1
    individuals_time=$(cat ${d}/statistics/breakdown.txt | grep "\<individuals\>" | grep -m1 successful | awk -F' ' '{print $5}')
    merge_time=$(cat ${d}/statistics/breakdown.txt | grep "\<individuals_merge\>" | grep -m1 successful | awk -F' ' '{print $5}') 
    runtime=$(bc <<<"$merge_time + $individuals_time")
    echo ${runtime}
}

function decaf_runtime {
    d=$1
    ctime=$(cat ${d}/00/00/merge_cluster1.out | tail -1 | sed 's/.*: //')
    echo "${ctime}"
}

function pmc_runtime {
    d=$1
    ctime=$(cat ${d}/00/00/merge_cluster1.err.* | grep "Wall time" | sed 's/.*Wall time: \([^ ]*\).*/\1/')
    echo "${ctime}"
}

function get_stats {
    cat $1 | grep "$2" | awk -F',' '{if($3!=""){count++;sum+=$3};y+=$3^2} END{sq=sqrt(y/NR-(sum/NR)^2);sq=sq?sq:0;print "mean = "sum/count ORS "std = ",sq}'
}

function generate_stats {
    echo "Scenario,Number of individuals,Makespan"
    for dir in ${LOG_DIR}/*/; do
        dir_name=$(basename $dir)
        ninds=$(echo ${dir_name} | awk -F[_.] '{print $2}')
        
        scenario=""
        case $dir_name in
            *"pegasus"*)
                scenario="Pegasus only"
                echo "${scenario},${ninds},$(pegasus_runtime $dir)"
                ;;
            *"decaf"*)
                scenario="Pegasus + Decaf"
                echo "${scenario},${ninds},$(decaf_runtime $dir)"
                ;;
            *"pmc"*)
                scenario="PMC"
                echo "${scenario},${ninds},$(pmc_runtime $dir)"
                ;;
            *)
                echo "Scenario should be either pegasus, decaf or pmc"
                ;;
        esac
    done
}

generate_stats > ${stat_file}
opts=( "Pegasus only" "Pegasus + Decaf" "PMC" )
sizes=( 2 5 10 16 )
for opt in "${opts[@]}"; do
    for size in "${sizes[@]}"; do
        echo ${opt}_${size}
        get_stats ${stat_file} "${opt},${size}"
    done
done