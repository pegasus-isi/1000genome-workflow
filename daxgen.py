#!/usr/bin/env python
import sys
import os
import csv
from datetime import datetime
from Pegasus.DAX3 import *
from optparse import OptionParser

# API Documentation: http://pegasus.isi.edu/documentation

# Options
parser = OptionParser(usage="usage: %prog [options]", version="%prog 1.0")

parser.add_option('-d', '--dax', action='store', dest='daxfile', default='1000genome.dax', help='DAX filename')
parser.add_option('-D', '--dataset', action='store', dest='dataset', default='20130502', help='Dataset folder')
parser.add_option('-f', '--datafile', action='store', dest='datafile', default='data.csv', help='Data file with list of input data')

(options, args) = parser.parse_args()

base_dir = os.path.abspath('.')

# Create a abstract dag
ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
#workflow = ADAG("1000genome-%s" % options.dataset)
workflow = ADAG("1000genome-%s" % ts)

# Executables
e_individuals = Executable('individuals', arch='x86_64', installed=False)
e_individuals.addPFN(PFN('file://' + base_dir + '/bin/individuals', 'local'))
workflow.addExecutable(e_individuals)

e_individuals_merge = Executable('individuals_merge', arch='x86_64', installed=False)
e_individuals_merge.addPFN(PFN('file://' + base_dir + '/bin/individuals_merge', 'local'))
workflow.addExecutable(e_individuals_merge)

e_sifting = Executable('sifting', arch='x86_64', installed=False)
e_sifting.addPFN(PFN('file://' + base_dir + '/bin/sifting', 'local'))
workflow.addExecutable(e_sifting)

e_mutation = Executable('mutation_overlap', arch='x86_64', installed=False)
e_mutation.addPFN(PFN('file://' + base_dir + '/bin/mutation_overlap.py', 'local'))
workflow.addExecutable(e_mutation)

e_freq = Executable('frequency', arch='x86_64', installed=False)
e_freq.addPFN(PFN('file://' + base_dir + '/bin/frequency.py', 'local'))
workflow.addExecutable(e_freq)

f_columns = File('columns.txt')
f_columns.addPFN(PFN('file://' + base_dir + '/data/' + options.dataset + '/columns.txt', 'condorpool'))
workflow.addFile(f_columns)

# Population Files
populations = []
for base_file in os.listdir('data/populations'):
  f_pop = File(base_file)
  f_pop.addPFN(PFN('file://' + base_dir + '/data/populations' + '/' + base_file, 'local'))
  workflow.addFile(f_pop)
  populations.append(f_pop)

f = open(options.datafile)
datacsv = csv.reader(f)
step = 1000
c_nums = []
individuals_files = []
sifted_files = []
sifted_jobs = []
individuals_merge_jobs = []

for row in datacsv:
  base_file = row[0]
  threshold = int(row[1])
  counter = 1
  individuals_jobs = []
  output_files = []

  # Individuals Jobs
  f_individuals = File(base_file)
  f_individuals.addPFN(PFN('file://' + base_dir + '/data/' + options.dataset + '/' + base_file, 'condorpool'))
  workflow.addFile(f_individuals)

  c_num = base_file[base_file.find('chr')+3:]
  c_num = c_num[0:c_num.find('.')]
  c_nums.append(c_num)

  while counter < threshold:
    stop = counter + step

    out_name = 'chr%sn-%s-%s.tar.gz' % (c_num, counter, stop)
    output_files.append(out_name)
    f_chrn = File(out_name)

    j_individuals = Job(name='individuals')
    j_individuals.uses(f_individuals, link=Link.INPUT)
    j_individuals.uses(f_columns, link=Link.INPUT)
    j_individuals.uses(f_chrn, link=Link.OUTPUT, transfer=False, register=False)
    j_individuals.addArguments(f_individuals, c_num, str(counter), str(stop), str(threshold))

    individuals_jobs.append(j_individuals)
    workflow.addJob(j_individuals)

    counter = counter + step

  # merge job
  j_individuals_merge = Job(name='individuals_merge')
  j_individuals_merge.addArguments(c_num)

  for out_name in output_files:
    f_chrn = File(out_name)
    j_individuals_merge.uses(f_chrn, link=Link.INPUT)
    j_individuals_merge.addArguments(f_chrn)

  individuals_filename = 'chr%sn.tar.gz' % c_num
  f_chrn_merged = File(individuals_filename)
  individuals_files.append(f_chrn_merged)
  j_individuals_merge.uses(f_chrn_merged, link=Link.OUTPUT, transfer=False, register=False)

  workflow.addJob(j_individuals_merge)
  individuals_merge_jobs.append(j_individuals_merge)

  for job in individuals_jobs:
    workflow.depends(j_individuals_merge, job)

  # Sifting Job
  j_sifting = Job(name='sifting')
  
  f_sifting = File(row[2])
  f_sifting.addPFN(PFN('file://' + base_dir +'/data/' + options.dataset + '/sifting/' + row[2], 'condorpool'))
  workflow.addFile(f_sifting)

  f_sifted = File('sifted.SIFT.chr%s.txt' % c_num)
  sifted_files.append(f_sifted)

  j_sifting = Job(name='sifting')
  j_sifting.uses(f_sifting, link=Link.INPUT)
  j_sifting.uses(f_sifted, link=Link.OUTPUT, transfer=False, register=False)
  j_sifting.addArguments(f_sifting, c_num)

  workflow.addJob(j_sifting)
  sifted_jobs.append(j_sifting)

# Analyses jobs
for i in range(len(individuals_files)):
  for f_pop in populations:
    # Mutation Overlap Job
    j_mutation = Job(name='mutation_overlap')
    j_mutation.addArguments('-c', c_nums[i], '-pop', f_pop)
    j_mutation.uses(individuals_files[i], link=Link.INPUT)
    j_mutation.uses(sifted_files[i], link=Link.INPUT)
    j_mutation.uses(f_pop, link=Link.INPUT)
    j_mutation.uses(f_columns, link=Link.INPUT)

    f_mut_out = File('chr%s-%s.tar.gz' % (c_nums[i], f_pop.name))
    j_mutation.uses(f_mut_out, link=Link.OUTPUT, transfer=True, register=False)
    
    workflow.addJob(j_mutation)
    workflow.depends(j_mutation, individuals_merge_jobs[i])
    workflow.depends(j_mutation, sifted_jobs[i])

    # Frequency Mutations Overlap Job
    j_freq = Job(name='frequency')
    j_freq.addArguments('-c', c_nums[i], '-pop', f_pop)
    j_freq.uses(individuals_files[i], link=Link.INPUT)
    j_freq.uses(sifted_files[i], link=Link.INPUT)
    j_freq.uses(f_pop, link=Link.INPUT)
    j_freq.uses(f_columns, link=Link.INPUT)

    f_freq_out = File('chr%s-%s-freq.tar.gz' % (c_nums[i], f_pop.name))
    j_freq.uses(f_freq_out, link=Link.OUTPUT, transfer=True, register=False)

    workflow.addJob(j_freq)
    workflow.depends(j_freq, individuals_merge_jobs[i])
    workflow.depends(j_freq, sifted_jobs[i])

# Write the DAX to file
f = open(options.daxfile, "w")
workflow.writeXML(f)
f.close()
