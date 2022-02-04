#!/usr/bin/env python3

import time

tic = time.perf_counter()
import numpy as np
import numpy.ma as ma
from random import sample
import os.path
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.cm as cm
import itertools
import argparse
import collections
from collections import Counter
from mpl_toolkits.axes_grid1 import make_axes_locatable
import itertools
import argparse
from pylab import pcolor, show, colorbar, xticks, yticks
from matplotlib import pyplot
import matplotlib as mpl

c_help = 'type a chromosome 1-22'
pop_help = 'type a population 0-6; 0:ALL, 1:EUR, 2:EAS, 3:AFR, 4:AMR, 5:SAS, 6:GBR'
description = 'Process mutation sets (-c and -POP are required).'
parser = argparse.ArgumentParser(description=description)
parser.add_argument("-c", type=int, help=c_help)
parser.add_argument("-pop", help=pop_help)
args = parser.parse_args()
c = args.c

SIFT = 'NO-SIFT'

n_runs = 1000
n_indiv = 52

siftfile = './sifted.SIFT.chr' + str(c) + '.txt'
data_dir = './'
pop_dir = './'
outdata_dir = "./chr{0}-{1}-freq/output_no_sift/".format(str(args.c), str(args.pop)) 
plot_dir = "./chr{0}-{1}-freq/plots_no_sift/".format(str(args.c), str(args.pop)) 

if not os.path.exists(outdata_dir):
    os.makedirs(outdata_dir, exist_ok=True)
if not os.path.exists(plot_dir):
    os.makedirs(plot_dir, exist_ok=True)

OutputFormat = '.png'

POP = args.pop
chrom = 'chr' + str(c)

font = {'family': 'serif', 'size': 14}
plt.rc('font', **font)

# untar input data
import tarfile

tar = tarfile.open(chrom + 'n.tar.gz')
tar.extractall(path='./' + chrom + 'n')
tar.close()


class ReadData:
    def read_names(self, POP):
        print('reading inidviduals')
        tic = time.perf_counter()
        namefile = pop_dir + POP
        f = open(namefile, 'r')
        text = f.read()
        f.close()
        text = text.split()
        all_ids = text[0:]
        file = data_dir + 'columns.txt'
        f = open(file, 'r')
        text = f.read()
        f.close()
        genome_ids = text.split()

        ids = list(set(all_ids) & set(genome_ids))
        print('time: %s' % (time.perf_counter() - tic))
        return ids

    def read_rs_numbers(self, siftfile):
        print('reading in rs with sift scores below %s' % SIFT)
        ## NB This file is in the format of:
        ## line number, rs number, ENSG number, SIFT, Phenotype
        tic = time.perf_counter()
        rs_numbers = []
        variations = {}
        map_variations = {}
        all_variations = []
        sift_file = open(siftfile, 'r')
        for item in sift_file:
            item = item.split()
            if len(item) > 2:
                rs_numbers.append(item[1])
                map_variations[item[1]] = item[2]
                variations[item[0]] = item[2]

        print('time: %s' % (time.perf_counter() - tic))
        return rs_numbers, map_variations

    def read_individuals(self, ids, rs_numbers):
        print('reading in individual mutation files')
        tic = time.perf_counter()
        mutation_index_array = []
        for name in ids:
            filename = data_dir + chrom + 'n/' + chrom + '.' + name
            f = open(filename, 'r')
            text = []
            for item in f:
                item = item.split()
                try:
                    text.append(item[1])
                except IndexError as e:
                    print("ERROR({}): while reading {}: (item: {})".format(str(e), filename, item))        
            sifted_mutations = list(set(rs_numbers).intersection(text))
            mutation_index_array.append(sifted_mutations)

        print('time: %s' % (time.perf_counter() - tic))
        return mutation_index_array


class Results:

    def overlap_ind(self, ids, mutation_index_array):
        n_p = len(mutation_index_array)
        print('calculating the number overlapings mutations between %s individuals selected randomly' % n_p)
        tic = time.perf_counter()
        list_p = np.linspace(0, n_p - 1, n_p).astype(int)
        mutation_overlap = []
        random_indiv = []
        for run in range(n_runs):
            randomized_list = sample(list(list_p), n_p)
            result = Counter()
            r_ids = []
            for pq in range(n_indiv):
                if 2 * pq >= len(randomized_list):
                    break
                b_multiset = collections.Counter(mutation_index_array[randomized_list[2 * pq]])
                print('time, inidividual: %s' % ids[randomized_list[2 * pq]])
                r_ids.append(ids[randomized_list[2 * pq]])
                result = result + b_multiset
            random_indiv.append(r_ids)
            mutation_overlap.append(result)
        print('time: %s' % (time.perf_counter() - tic))
        return mutation_overlap, random_indiv

    def histogram_overlap(self, mutation_overlap):
        print('calculating the frequency/historgram of overlapings mutations')
        tic = time.perf_counter()
        histogram_overlap = []
        for run in range(n_runs):
            final_counts = [count for item, count in mutation_overlap[run].items()]
            histogram_overlap.append(collections.Counter(final_counts))
        print('time: %s' % (time.perf_counter() - tic))
        return histogram_overlap


class PlotData:

    def plot_histogram_overlap(self, POP, histogram_overlap, outputFile):
        print('ploting Histogram mutation overlap to %s' % outputFile)
        tic = time.perf_counter()
        for run in range(n_runs):
            output = outputFile + str(run) + '.png'
            final_counts = [count for item, count in histogram_overlap[run].items()]
            N = len(final_counts)
            x = range(N)
            width = 1 / 1.5
            bar1 = plt.bar(x, final_counts, width, color="grey")
            plt.ylabel('Mutations')
            plt.xlabel('Individuals')
            plt.xticks(np.arange(1, N + 1))
            plt.savefig(output)
            plt.close()
        print('time: %s' % (time.perf_counter() - tic))


class WriteData:

    def write_histogram_overlap(self, histogram_overlapfile, histogram_overlap):
        print('writing Frequency historgram of mutations overlapping to %s' % histogram_overlapfile)
        tic = time.perf_counter()
        for run in range(n_runs):
            overlapfile = histogram_overlapfile + str(run) + '.txt'
            f = open(overlapfile, 'w')
            f.write('Number Individuals - Number Mutations  \n')
            for i in range(1, n_indiv + 1):
                if i in histogram_overlap[run]:
                    f.write(str(i) + '-' + str(histogram_overlap[run][i]) + '\n')
                else:
                    f.write(str(i) + '-' + str(0) + '\n')
            f.close()

        # for key, count in histogram_overlap[run].items() :
        #	f.write(str(key) + '-' + str(count) + '\n')
        print('time: %s' % (time.perf_counter() - tic))

    def write_mutation_overlap(self, mutation_overlapfile, mutation_overlap):
        print('writing Mutations overlapping to %s' % mutation_overlapfile)
        tic = time.perf_counter()
        for run in range(n_runs):
            overlapfile = mutation_overlapfile + str(run) + '.txt'
            f = open(overlapfile, 'w')
            f.write('Mutation Index- Number Overlapings \n')
            for key, count in mutation_overlap[run].items():
                f.write(key + '-' + str(count) + '\n')
            f.close()
        print('time: %s' % (time.perf_counter() - tic))

    def write_random_indiv(self, randomindiv_file, random_indiv):
        tic = time.perf_counter()
        for run in range(n_runs):
            randomfile = randomindiv_file + str(run) + '.txt'
            f = open(randomfile, 'w')
            print('writing Random individuals to %s' % randomfile)
            f.write('Individuals \n')
            for item in random_indiv[run]:
                f.write("%s\n" % item)
            f.close()
        print('time: %s' % (time.perf_counter() - tic))

    def write_mutation_index_array(self, mutation_index_array_file, mutation_index_array):
        print('writing Mutation index array to %s' % mutation_index_array_file)
        tic = time.perf_counter()
        f = open(mutation_index_array_file, "w")
        for item in mutation_index_array:
            f.write("%s\n" % item)
        f.close()
        print('time: %s' % (time.perf_counter() - tic))

    def write_map_variations(self, map_variations_file, map_variations):
        print('writing map_variations to %s' % map_variations_file)
        tic = time.perf_counter()
        f = open(map_variations_file, 'w')
        for key, count in map_variations.items():
            f.write(key + '\t' + str(count) + '\n')
        f.close()
        print('time: %s' % (time.perf_counter() - tic))


############################################################


if __name__ == '__main__':
    rd = ReadData()
    res = Results()
    wr = WriteData()
    pd = PlotData()

    histogram_overlapfile = outdata_dir + 'Histogram_mutation_overlap_chr' + str(c) + '_s' + \
                            str(SIFT) + '_' + POP + '_'
    mutation_overlapfile = outdata_dir + 'Mutation_overlap_chr' + str(c) + '_s' + \
                           str(SIFT) + '_' + POP + '_'
    mutation_index_array_file = outdata_dir + 'mutation_index_array' + str(c) + '_s' + \
                                str(SIFT) + '_' + POP + '.txt'
    histogram_overlap_plot = plot_dir + 'Frequency_mutations' + str(c) + '_s' + \
                             str(SIFT) + '_' + POP
    map_variations_file = outdata_dir + 'map_variations' + str(c) + '_s' + \
                          str(SIFT) + '_' + POP + '.txt'

    randomindiv_file = outdata_dir + 'random_indiv' + str(c) + '_s' + \
                       str(SIFT) + '_' + POP + '_'

    ids = rd.read_names(POP)
    n_pairs = len(ids) / 2

    rs_numbers, map_variations = rd.read_rs_numbers(siftfile)
    mutation_index_array = rd.read_individuals(ids, rs_numbers)

    wr.write_map_variations(map_variations_file, map_variations)
    wr.write_mutation_index_array(mutation_index_array_file, mutation_index_array)

    mutation_overlap, random_indiv = res.overlap_ind(ids, mutation_index_array)
    histogram_overlap = res.histogram_overlap(mutation_overlap)

    wr.write_mutation_overlap(mutation_overlapfile, mutation_overlap)
    wr.write_histogram_overlap(histogram_overlapfile, histogram_overlap)
    wr.write_random_indiv(randomindiv_file, random_indiv)

    pd.plot_histogram_overlap(POP, histogram_overlap, histogram_overlap_plot)

    # gen final output
    tar = tarfile.open('chr%s-%s-freq.tar.gz' % (c, POP), 'w:gz')
    tar.add(outdata_dir)
    tar.add(plot_dir)
    tar.close()
