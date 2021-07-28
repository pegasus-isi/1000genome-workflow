#!/usr/bin/env python3

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

def merging(c, tar_files):
    print('= Merging chromosome {}...'.format(c))
    tic = time.perf_counter()

    merged_dir = 'merged/'
    os.makedirs(merged_dir, exist_ok=True)

    data = {}

    for tar in tar_files:
        tic_iter = time.perf_counter()
        with tempfile.TemporaryDirectory(dir=os.curdir) as temp_dir:
            for file in extract_all(tar, temp_dir):
                content = readfile(temp_dir+'/'+file)
                if file in data:
                    data[file] += content
                else:
                    data[file] = content

        print("Merged {} in {:0.2f} sec".format(tar, time.perf_counter()-tic_iter))
    
    for file,content in data.items():
        writefile(merged_dir+'/'+file, content)
    
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
    merging(c=sys.argv[1], tar_files=sys.argv[2:])
