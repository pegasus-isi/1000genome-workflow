#!/usr/bin/env python3

import pybredala as bd
import pydecaf as d
from mpi4py import MPI

import os
import sys
import re
import time
import tarfile
import shutil


def compress(output, input_dir):
    with tarfile.open(output, "w:gz") as file:
        file.add(input_dir, arcname=os.path.basename(input_dir))

def readfile(file):
    with open(file, 'r') as f:
        content = f.readlines()
    return content

#orc@09-08: the things I omitted are with ##
def processing(inputfile, columfile, c, counter, stop, total):
    print('= Now processing chromosome: {}'.format(c))
    tic = time.perf_counter()

    counter = int(counter)
    ending = min(int(stop), int(total))

    ### step 0
    unzipped = 'ALL.chr{}.individuals.vcf'.format(c)
    # unzipped = os.path.splitext(inputfile)[0]  # Remove .gz

    # shutil.move(inputfile, unzipped)

    # if not os.path.exists(unzipped):
    #     decompress(inputfile, unzipped)

    rawdata = readfile(inputfile)

    ### step 2
    ##ndir = 'chr{}n/'.format(c)
    ##os.makedirs(ndir, exist_ok=True)

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

    chrp_data = {}
    filename_arr = []
    count_arr = []

    columndata = readfile(columfile)[0].rstrip('\n').split('\t')

    start_data = 9  # where the real data start, the first 0|1, 1|1, 1|0 or 0|0
    # position of the last element (normally equals to len(data[0].split(' '))
    #end_data = 2504
    end_data = len(columndata) - start_data
    print("== Number of columns {}".format(end_data))

    comm = MPI.COMM_WORLD

    for i in range(0, end_data):
        col = i + start_data
        name = columndata[col]
        ##filename = "{}/chr{}.{}".format(ndir, c, name)
        filename_mpi = "chr{}.{}".format(c, name)
        filename_arr.append(filename_mpi)
        print("=== Sending the file {} contents over MPI".format(filename_mpi), end=" => ")
        tic_iter = time.perf_counter()
        chrp_data[i] = []
        count = 0

##        with open(filename, 'w') as f:
        for line in data:
            #print(i, line.split('\t'))
            first = line.split('\t')[col]  # first =`echo $l | cut -d -f$i`
            #second =`echo $l | cut -d -f 2, 3, 4, 5, 8 --output-delimiter = '   '`
            second = line.split('\t')[0:8]
            # We select the one we want
            second = [elem for id, elem in enumerate(second) if id in [1, 2, 3, 4, 7]]
            af_value = second[4].split(';')[8].split('=')[1]
            # We replace with AF_Value
            second[4] = af_value
            #af_value = float(af_value.split(',')[0]) # We only keep the first value if more than one (that's what awk is doing)
            try:
                if ',' in af_value:
                    # We only keep the first value if more than one (that's what awk is doing)
                    af_value = float(af_value.split(',')[0])
                else:
                    af_value = float(af_value)
                    
                elem = first.split('|')
                # We skip some lines that do not meet these conditions
                if af_value >= 0.5 and elem[0] == '0':
                    chrp_data[i].append(second)
                    count = count + 1
                elif af_value < 0.5 and elem[0] == '1':
                    chrp_data[i].append(second)
                    count = count + 1
                else:
                    continue
            except ValueError:
                continue

##orc@09-08: first gains w the mpi version, eliminating the file write on individuals
##                f.write("{0}        {1}    {2}    {3}    {4}\n".format(
##                    second[0], second[1], second[2],second[3],second[4])
##               )

        tic_comm = time.perf_counter()
        count_arr.append(count)
        print("processed in {:0.2f} sec".format(time.perf_counter()-tic_iter))

    tic_comm = time.perf_counter()
    size = comm.Get_size()
    # orc@11-08: for now, assuming a 2-job workflow: multi indv + indv_merge
    send_dest = size-1
    ##orc@10-08: sending the filename, line count, and the data itself to indv_merge
    comm.send(filename_arr,dest=send_dest, tag=12)
    comm.send(count_arr,   dest=send_dest, tag=7)
    comm.send(chrp_data,   dest=send_dest, tag=13)

##orc@09-08: another gain w the mpi version -- eliminating compression and removal of temp files
##    outputfile = "chr{}n-{}-{}.tar.gz".format(c, counter, stop)
##    print("== Done. Zipping {} files into {}.".format(end_data, outputfile))

    # tar -zcf .. /$outputfile .
##    tic_compress = time.perf_counter()
##    compress(outputfile, ndir)
##    print("compressed in {:0.2f} sec".format(time.perf_counter()-tic_compress))

    # Cleaning temporary files
##    try:
##        shutil.rmtree(ndir)
##    except OSError as e:
##        print("Error: %s : %s" % (ndir, e.strerror))

##    print("compressed and removed temp files in {:0.2f} sec".format(time.perf_counter()-tic_compress))

    print("= Chromosome {} processed in {:0.2f} seconds and mpi send was {:0.2f} seconds.".format(c, time.perf_counter() - tic, time.perf_counter()-tic_comm))


if __name__ == "__main__":
    print(f"Host = {os.uname()[1]}")
    print(f"CPUs = {os.sched_getaffinity(0)}")
    start = time.time()
    inputfile = sys.argv[1]
    c = sys.argv[2]
    counter = sys.argv[3]
    stop = sys.argv[4]
    total = sys.argv[5]
    columfile = 'columns.txt'


    w = d.Workflow()
    w.makeWflow(w,"1Kgenome.json")

    a = MPI._addressof(MPI.COMM_WORLD)
    r = MPI.COMM_WORLD.Get_rank()
    decaf = d.Decaf(a,w)

    processing(inputfile=inputfile, 
            columfile=columfile, 
            c=c, 
            counter=counter, 
            stop=stop,
            total=total)

    print("individuals at rank " + str(r) + " terminating")
    decaf.terminate()
    print('Execution time in seconds: ' + str(time.time() - start))
