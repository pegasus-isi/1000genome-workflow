#!/usr/bin/env python3

import pybredala as bd
import pydecaf as d
from mpi4py import MPI

import os
import sys
import time
import tarfile
import shutil
import tempfile


def compress(archive, input_dir):
    with tarfile.open(archive, "w:gz") as file:
        file.add(input_dir, arcname=os.path.basename(input_dir))

def extract_all(archive, output_dir):
    with tarfile.open(archive, "r:*") as file:
        file.extractall(os.path.basename(output_dir))
        flist = file.getnames()
        if flist[0] == '':
            flist = flist[1:]
        return flist

def readfile(file):
    with open(file, 'r') as f:
        content = f.readlines()
    return content

def writefile(file, content):
    with open(file, 'w') as f:
        f.writelines(content)

#orc@09-08: the things I omitted are with ##
def merging(c, tar_files):
    print('= Merging chromosome {}...'.format(c))
    tic = time.perf_counter()

    merged_dir = 'merged/'
    os.makedirs(merged_dir, exist_ok=True)


    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    num_recv = size - 1

    data = {}
    end_data = 2504 #TODO we can fetch this via Decaf/mpi from individuals job as metadata if needed.

    for k in range(0, num_recv):

        filename_arr = comm.recv(source=k, tag=12)
        end_line_arr = comm.recv(source=k, tag=7)
        all_data = comm.recv(source=k, tag=13)

        for i in range(0, end_data):
##            tic_iter = time.perf_counter()
            file = filename_arr[i]
            end_line = end_line_arr[i]
            sub_data = all_data[i]
##orc@11-08: need to append in multi mode.
            with open(merged_dir+'/'+file, 'a') as f:
                for m in range(0,end_line):
                    second = sub_data[m]

                    f.write("{0}        {1}    {2}    {3}    {4}\n".format(
                            second[0], second[1], second[2],second[3],second[4])
                    )


##orc@09-08: first gain w the mpi version -- no need to decompress/read
##    for tar in tar_files:
##        tic_iter = time.perf_counter()
##        with tempfile.TemporaryDirectory(dir=os.curdir) as temp_dir:
##            for file in extract_all(tar, temp_dir):
##                content = readfile(temp_dir+'/'+file)
##                print("content:")
##                print(content)
##                print("file:")
##                print(file)
##                if file in data:
##                    data[file] += content
##                else:
##                    data[file] = content

##            print("Merged in {:0.2f} sec".format(time.perf_counter()-tic_iter))

    ##orc@09-08: another gain w the mpi version -- eliminating the file write
    ##tic_write = time.perf_counter()
    ##for file,content in data.items():
    ##    writefile(merged_dir+'/'+file, content)

    ##print("Wrote in {:0.2f} sec".format(time.perf_counter()-tic_write))

    outputfile = "chr{}n.tar.gz".format(c)
    print("== Done. Zipping {} files into {}.".format(len(data), outputfile))

    compress(outputfile, merged_dir)

    # Cleaning temporary files
    try:
        shutil.rmtree(merged_dir)
    except OSError as e:
        print("Error: %s : %s" % (merged_dir, e.strerror))

    print("= Chromosome {} merged in {:0.2f} seconds.".format(
        c, time.perf_counter() - tic))


if __name__ == "__main__":
    print(f"Host = {os.uname()[1]}")
    print(f"CPUs = {os.sched_getaffinity(0)}")
    start = time.time()
    w = d.Workflow()
    w.makeWflow(w,"1Kgenome.json")

    a = MPI._addressof(MPI.COMM_WORLD)
    r = MPI.COMM_WORLD.Get_rank()
    decaf = d.Decaf(a,w)

    merging(c=sys.argv[1], tar_files=sys.argv[2:])

    print("individuals_merge at rank " + str(r) + " terminating")
    decaf.terminate()
    print('Execution time in seconds: ' + str(time.time() - start))