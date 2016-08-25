#!/usr/bin/env python
import sys
import os
import csv
from Pegasus.DAX3 import *

# API Documentation: http://pegasus.isi.edu/documentation

if len(sys.argv) != 4:
    sys.stderr.write("Usage: %s DAXFILE DATASET DATACSV\n" % (sys.argv[0]))
    exit(1)

daxfile = sys.argv[1]
dataset = sys.argv[2]
datafile = sys.argv[3]

base_dir = os.path.abspath('.')

# Create a abstract dag
workflow = ADAG("1000genome-%s" % dataset)

# Executable
e_individuals = Executable('individuals', arch='x86_64', installed=False)
e_individuals.addPFN(PFN('file://' + base_dir + '/bin/individuals', 'local'))
workflow.addExecutable(e_individuals)

# Individuals Jobs
f = open(datafile)
datacsv = csv.reader(f)
step = 1000
threshold = 10000

for row in datacsv:
  base_file = row[0]
  num_lines = int(row[1])
  counter = 1
  output_files = []

  f_individuals = File(base_file)
  f_individuals.addPFN(PFN('file://' + os.path.abspath('data/%s' % dataset) + '/' + base_file, 'local'))
  workflow.addFile(f_individuals)

  while counter < threshold:
    c_num = base_file[base_file.find('chr')+3:]
    c_num = c_num[0:c_num.find('.')]
    stop = counter + step

    out_name = 'chr%sn-%s-%s.tar.gz' % (c_num, counter, stop)
    output_files.append(out_name)
    f_chrn = File(out_name)

    j_individuals = Job(name='individuals')
    j_individuals.uses(f_individuals, link=Link.INPUT)
    j_individuals.uses(f_chrn, link=Link.OUTPUT, transfer=True)
    j_individuals.addArguments(f_individuals, c_num, str(counter), str(stop), str(threshold))

    workflow.addJob(j_individuals)

    counter = counter + step

# Write the DAX to file
f = open(daxfile, "w")
workflow.writeXML(f)
f.close()
