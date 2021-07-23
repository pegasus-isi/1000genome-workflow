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

    counter = int(counter)
    ending = int(min(stop, total))

    ### step 0
    nfile = 'ALL.chr{}.individuals.vcf.gz'.format(c)
    unzipped = os.path.splitext(nfile)[0] # Remove .gz

    if not os.path.exists(unzipped):
        decompress(nfile, unzipped)

    rawdata = readfile(unzipped)

    ### step 2
    pdir = 'chr{}p/'.format(c)
    ndir = 'chr{}n/'.format(c)

    # mkdir -p $pdir
    os.makedirs(pdir, exist_ok=True)
    # mkdir -p $ndir
    os.makedirs(ndir, exist_ok=True)

    ### step 3
    # In the bash version, counter started at 1 but in Python we start at 0. 
    # counter = max(0, counter - 1)  # The max ensure that we don't do -1 if the user set counter 0 directly
    print("== Total number of lines: {}".format(total))
    print("== Processing {} from line {} to {}".format(unzipped, counter, stop))

    # We consider the line from counter to stop and we don't over total, then we remove lines starting with '#'
    #sed -n "$counter"','"$stop"'p;'"$total"'q' $unzipped | grep -ve "#" > cc
    regex = re.compile('(?!#)')
    # print(counter, min(stop, total), data[int(counter):int(min(stop, total))] )
    data = list(filter(regex.match, rawdata[counter:ending]))
    data = [x.rstrip('\n') for x in data] # Remove \n from words 

    ### Stopped here 
    start_data = 9  # where the real data start, the first 0|1, 1|1, 1|0 or 0|0
    # position of the last element (normally equals to len(data[0].split(' '))
    end_data = 2513
    # end_data = 15

    for i in range(start_data, end_data+1):
        filename = "chr{}p/chr{}.p{}".format(c, c, i - start_data + 1)
        print("=== Writing file {}".format(filename))
        with open(filename, 'w') as f:
            for line in data:
                #print(i, line.split('\t'))
                first = line.split('\t')[i]  # first =`echo $l | cut -d -f$i`
                #second =`echo $l | cut -d -f 2, 3, 4, 5, 8 --output-delimiter = '   '`
                second = line.split('\t')[0:8]
                # We select the one we want
                second = '   '.join([elem for id, elem in enumerate(
                    second) if id in [1, 2, 3, 4, 7]])
                #echo "$first    $second" >> chr${c}p/chr${c}.p$((i - 9))
                f.write("{}    {}\n".format(first, second))



if __name__ == "__main__":
    inputfile = sys.argv[1]
    c = sys.argv[2]
    counter = sys.argv[3]
    stop = sys.argv[4]
    total = sys.argv[5]
    columfile = 'columns.txt'

    processing(inputfile, columfile, c, counter, stop, total)

