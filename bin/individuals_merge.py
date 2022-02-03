#!/usr/bin/env python3

import os
import sys
import time
import tarfile
import shutil
import tempfile


def compress(archive, input_dir):
    with tarfile.open(archive, "w:gz") as f:
        f.add(input_dir, arcname="")

def extract_all(archive, output_dir):
    with tarfile.open(archive, "r:*") as f:
        f.extractall(os.path.basename(output_dir))
        flist = f.getnames()
        if flist[0] == '':
            flist = flist[1:]
        return flist

def readfile(filename):
    with open(filename, 'r') as f:
        content = f.readlines()
    return content

def writefile(filename, content):
    with open(filename, 'w') as f:
        f.writelines(content)

def merging(c, tar_files):
    print('= Merging chromosome {}...'.format(c))
    tic = time.perf_counter()

    merged_dir = "merged_chr{}".format(c)
    os.makedirs(merged_dir, exist_ok=True)

    data = {}

    for tar in tar_files:
        tic_iter = time.perf_counter()
        with tempfile.TemporaryDirectory(dir=os.curdir) as temp_dir:
            for filename in extract_all(tar, temp_dir):
                content = readfile(os.path.join(temp_dir, filename))
                if filename in data:
                    data[filename] += content
                else:
                    data[filename] = content

        print("Merged {} in {:0.2f} sec".format(tar, time.perf_counter()-tic_iter))
    
    for filename,content in data.items():
        writefile(os.path.join(merged_dir, filename), content)
    
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
    merging(c=sys.argv[1], tar_files=sys.argv[2:])
