#!/usr/bin/env python
import sys
import os
from Pegasus.DAX3 import *

# API Documentation: http://pegasus.isi.edu/documentation

if len(sys.argv) != 3:
    sys.stderr.write("Usage: %s DAXFILE DATASET\n" % (sys.argv[0]))
    exit(1)

daxfile = sys.argv[1]
dataset = sys.argv[2]

base_dir = os.path.abspath('.')

# Create a abstract dag
workflow = ADAG("1000genome-%s" % dataset)

# Executable
e_individuals = Executable('individuals', arch='x86_64', installed=False)
e_individuals.addPFN(PFN('file://' + base_dir + '/bin/individuals', 'local'))
workflow.addExecutable(e_individuals)

# Individuals Jobs
for base_file in os.listdir('data/%s' % dataset):
  j_individuals = Job(name='individuals')

  f_individuals = File(base_file)
  f_individuals.addPFN(PFN('file://' + os.path.abspath('data/%s' % dataset) + '/' + base_file, 'local'))
  workflow.addFile(f_individuals)

  c_num = base_file[base_file.find('chr')+3:]
  c_num = c_num[0:c_num.find('.')]

  j_individuals.uses(f_individuals, link=Link.INPUT)
  j_individuals.addArguments(f_individuals, c_num)
  workflow.addJob(j_individuals)

# Write the DAX to file
f = open(daxfile, "w")
workflow.writeXML(f)
f.close()
