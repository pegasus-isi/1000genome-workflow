#!/usr/bin/env python

import os
import sys
import gzip
import re

def compress(src, dest):
    with open(src, 'rw') as s, open(dest, 'wb', encoding='utf8') as d:
        data = gzip.compress(s.read().decode('utf-8'))
        d.write(dest)

def decompress(src, dest):
    with open(src, 'rb') as s, open(dest, 'w', encoding='utf8') as d:
        data = gzip.decompress(s.read()).decode('utf-8')
        d.write(dest)

def readfile(file):
    with open(file, 'r') as f:
        content = f.readlines()
    return content

def processing(columfile, inputfile, c, counter, stop, total):
    print('= Now processing chromosome: {}'.format(c))

    ### step 0
    nfile = 'ALL.chr{}.individuals.vcf.gz'.format(c)
    unzipped = os.path.splitext(nfile)[0] # Remove .gz

    if not os.path.exists(unzipped):
        decompress(nfile, unzipped)

    data = readfile(unzipped)

    ### step 2
    pdir = 'chr{}p/'.format(c)
    ndir = 'chr{}n/'.format(c)

    # mkdir -p $pdir
    # mkdir -p $ndir

    ### step 3
    # In the bash version, counter started at 1 but in Python we start at 0. 
    # counter = max(0, counter - 1)  # The max ensure that we don't do -1 if the user set counter 0 directly
    print("== Total number of lines: {}".format(total))
    print("== Processing {} from line {} to {}".format(unzipped, counter, stop))

    # We consider the line from counter to stop and we don't over total, then we remove lines starting with '#'
    #sed -n "$counter"','"$stop"'p;'"$total"'q' $unzipped | grep -ve "#" > cc
    regex = re.compile('(?!#)')
    chunk = list(filter(regex.match, data[counter:min(stop, total)]))

    ### Stopped here
    # for i in {10..2513}
    # do
    #     while read l
    #     do
    #         first =`echo $l | cut - d - f$i`
    #         second =`echo $l | cut - d - f 2, 3, 4, 5, 8 - -output-delimiter = '   '`
    #         echo "$first    $second" >> chr${c}p/chr${c}.p$((i - 9))
    #     done < cc
    # done

if __name__ == "__main__":
    inputfile = sys.argv[1]
    c = sys.argv[2]
    counter = sys.argv[3]
    stop = sys.argv[4]
    total = sys.argv[5]
    columfile = 'columns.txt'

    processing(inputfile, columfile, c, counter, stop, total)
   
