#!/usr/bin/env python3

import os
import sys
import re
import time
import subprocess

def readfile(file):
    with open(file, 'r') as f:
        content = f.readlines()
    return content

def sifting(inputfile, c):
    tic = time.perf_counter()

    # unzipped = 'ALL.chr{}.vcf'.format(c)
    final = 'sifted.SIFT.chr{}.txt'.format(c)

    rawdata = readfile(inputfile)

    print("= Taking columns from {}".format(inputfile))
    print("== Filtering over {} lines".format(len(rawdata)))

    # header =$(head - n 1000 $unzipped | grep "#" | wc - l)
    r1 = re.compile('.*(#).*')
    header = len(list(filter(r1.match, rawdata[:1000])))
    print("== Header found -> {}".format(header))

    # grep -n "deleterious\|tolerated" $unzipped | grep "rs" > $siftfile
    ## This regex is super slow
    # r2 = re.compile('.*(deleterious|tolerated).*')
    # data_temp = list(filter(r2.match, rawdata))
    # data_temp = []

    # init_size = len(rawdata)

    # for lineno,content in enumerate(rawdata[header:]):
    #     if r2.search(content):
    #         data_temp.append(str(lineno)+':'+content)
    #     print('{}/{}'.format(lineno, init_size), end='\r')

    siftfile = 'SIFT.chr{}.vcf'.format(c)
    with open(siftfile, 'w') as f:
        subprocess.run(["grep -n \"deleterious\|tolerated\" {}".format(inputfile)], shell=True, stdout=f)

    data_temp = readfile(siftfile)

    r3 = re.compile('.*(rs).*')
    data = list(filter(r3.match, data_temp))

    print("== Starting processing {} lines".format(len(data)))

    with open(final, 'w') as f:
        for l in data:
            # awk '{print $1}' $siftfile | awk -F ":" '{print $1-'$header'}' > $lines #.txt
            line = str(int(l.split('\t')[0].split(':')[0]) - int(header))
            # awk '{print $3}' $siftfile > $ids #.txt
            id = l.split('\t')[2]

            # awk '{print $8}' $siftfile > $info  # .txt
            # awk - F "|" '{print $5"\t"$17"\t"$18}' $info | sed 's/(/\t/g' | sed 's/)//g' > $sifts
            sifts = l.split('\t')[7].split('|')
            sifts = sifts[4] + ' ' + sifts[16] + ' ' + sifts[17]
            sifts = sifts.replace('(', ' ').replace(')', '')
            
            # pr -m -t -s ' ' $lines $ids $sifts | gawk '{print $1,$2,$3,$5,$7}' > $final
            temp = (line + ' ' + id + ' ' + sifts).split(' ')

            if temp[3] == '' or temp[4] == '':
                f.write("{} {} {}\n".format(temp[0], temp[1], temp[2]))
            elif temp[5] == '':
                f.write("{} {} {} {}\n".format(temp[0], temp[1], temp[2], temp[4]))
            else:
                f.write("{} {} {} {} {}\n".format(temp[0], temp[1], temp[2], temp[4], temp[6]))

    os.remove(siftfile)
    print("= Line, id, ENSG id, SIFT, and phenotype printed to {} in {:0.2f} seconds.".format(final, time.perf_counter() - tic))

if __name__ == "__main__":
    sifting(inputfile=sys.argv[1], c=sys.argv[2])
