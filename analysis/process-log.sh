#!/bin/sh

# pip3 install yq
# brew install jq

echo "K,Trial,JobID,Size(B),MAXRSS(KB)"

for directory in pegasus_*; do
    temp=${directory//[^0-9.]/}
    k=$(echo $temp | cut -d. -f1)
    trial=$(echo $temp | cut -d. -f2)

    for f in $directory/00/00/individuals_ID*.out.*; do
        run=${f##$directory/00/00/individuals_}
        if [ -s $f ]; then
            run=${run%%.out.000}
            run=${run%%.out.001}
            size=$(cat $f | yq '.[0]' | jq '.files | to_entries[] | select(.key | startswith("chr")).value.size')
            maxrss=$(cat $f | yq '.[0]' | jq '.mainjob.usage.maxrss')
            echo "$k,$trial,$run,$size,$maxrss"
        fi
    done
done

