#!/usr/bin/env python

import os
import sys
import gzip
import re
import shutil

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

def processing(inputfile, columfile, c, counter, stop, total):
    print('= Now processing chromosome: {}'.format(c))

    counter = int(counter)
    ending = int(min(stop, total))

    ### step 0
    unzipped = 'ALL.chr{}.individuals.vcf'.format(c)
    # unzipped = os.path.splitext(inputfile)[0]  # Remove .gz

    # shutil.move(inputfile, unzipped)

    # if not os.path.exists(unzipped):
    #     decompress(inputfile, unzipped)

    rawdata = readfile(inputfile)

    ### step 2
    # pdir = 'py-chr{}p/'.format(c)
    ndir = 'py-chr{}n/'.format(c)

    # mkdir -p $pdir
    # os.makedirs(pdir, exist_ok=True)
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

    start_data = 9  # where the real data start, the first 0|1, 1|1, 1|0 or 0|0
    # position of the last element (normally equals to len(data[0].split(' '))
    end_data = 2513
    # end_data = 14

    chrp_data = {}
    columndata = readfile(columfile)[0].rstrip('\n').split('\t')

    for i in range(start_data, end_data+1):
        name = columndata[i]

        filename = "{}/chr{}.{}".format(ndir, c, name)
        print("=== Writing file {}".format(filename))
        chrp_data[i - start_data] = []

        with open(filename, 'w') as f:
            for line in data:
                #print(i, line.split('\t'))
                first = line.split('\t')[i]  # first =`echo $l | cut -d -f$i`
                #second =`echo $l | cut -d -f 2, 3, 4, 5, 8 --output-delimiter = '   '`
                second = line.split('\t')[0:8]
                # We select the one we want
                second = [elem for id, elem in enumerate(second) if id in [1, 2, 3, 4, 7]]
                af_value = second[4].split(';')[8].split('=')[1]
                # We replace with AF_Value
                second[4] = af_value
                af_value = float(af_value.split(',')[0]) # We only keep the first value if more than one (that's what awk is doing)
                
                elem = first.split('|')
                # We skip some lines that do not meet these conditions
                if af_value >= 0.5 and (elem[0] == '0'):
                    chrp_data[i-start_data].append(second)
                elif af_value < 0.5 and (elem[0] == '1'):
                    chrp_data[i-start_data].append(second)
                else:
                    continue
                
                f.write("{0}        {1}    {2}    {3}    {4}\n".format(
                    second[0], second[1], second[2],second[3],second[4])
                )


                # chrp_data[i-start_data].append([first]+second)

                #echo "$first    $second" >> chr${c}p/chr${c}.p$((i - 9))
                # f.write("{}    {}\n".format(first, '   '.join(second)))


    # start = 0
    # end = 2504
    # end = 10
    
    # result = {}
    # for i in range(start, end+1):
    #     # col=$(($i + 9))
    #     col = i + 9
    #     #name =$(cut -f $col $columfile)
    #     name = columndata[col]
    #     #oldfile =$pdir'chr'$c'.p'$i
    #     oldfile = "{}/chr{}.p{}".format(pdir, c, i)
    #     #newfile =$ndir'chr'$c'.'$name
    #     newfile = "{}/chr{}.p{}".format(ndir, c, name)
    #     # cat $oldfile | awk '{print $6}' | awk - F ";" '{print $9}' | awk - F"=" '{print $2}' > AF_value.$c
    #     result[i] = {'AF_value': []}
        
    #     for line in chrp_data[i]:
    #         af_value = line[5].split(';')[8].split('=')[1]
    #         result[i]['AF_value'].append(af_value)


if __name__ == "__main__":
    inputfile = sys.argv[1]
    c = sys.argv[2]
    counter = sys.argv[3]
    stop = sys.argv[4]
    total = sys.argv[5]
    columfile = 'columns.txt'

    processing(inputfile=inputfile, 
            columfile=columfile, 
            c=c, 
            counter=counter, 
            stop=stop,
            total=total)

