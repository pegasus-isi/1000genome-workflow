# 1000Genomes Workflow

The 1000 genomes project provides a reference for human variation, having reconstructed the genomes of 2,504 individuals across 26 different populations to energize these approaches. This workflow identifies mutational overlaps using data from the 1000 genomes project in order to provide a null distribution for rigorous statistical evaluation of potential disease-related mutations. The workflow fetchs, parses, and analyzes data from the [1000 genomes Project] (ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/). It cross-matches the extracted data (which person has which mutations), with the mutation's sift score (how bad it is). Then it performs a few analyses, including plotting.

The figure below shows a branch of the workflow for the analysis of a single chromossome.

<p align="center">
  <img src="docs/images/1000genome.png?raw=true" width="450">
</p>

_Individuals_. This task fetches and parses the Phase3 data from the 1000 genomes project by chromosome. These files list all of Single nucleotide polymorphisms (SNPs) variants in that chromosome and which individuals have each one. SNPs are the most common type of genetic variation among people, and are the ones we consider in this work. An individual task creates output files for each individual of _rs numbers_ 3, where individuals have mutations on both alleles.

_Populations_. The 1000 genome project has 26 different populations from many different locations around the globe. A population task downloads a file per population selected. This workflow uses five super populations: African (AFR), Mixed American (AMR), East Asian (EAS), European (EUR), and South Asian (SAS). The workflow also uses ALL population, which means that all individuals from the latest release are considered.

_Sifting_. A sifting task computes the SIFT scores of all of the SNPs variants, as computed by the Variant Effect Predictor (VEP). SIFT is a sequence homology-based tool that Sorts Intolerant From Tolerant amino acid substitutions, and predicts whether an amino acid substitution in a protein will have a phenotypic effect. For each chromosome the sifting task processes the corresponding VEP, and selects only the SNPs variants that has a SIFT score, recording in a file (per chromosome) the SIFT score and the SNPs variants ids, which are: (1) rs number, (2) ENSEMBL GEN ID, and (3) HGNC ID.

_Pair Overlap Mutations_. This task measures the overlap in mutations (also called SNPs variants) among pairs of individuals by population and by chromosome.

_Frequency Overlap Mutations_. This tasks measures the frequency of overlapping in mutations by selecting a number of random individuals, and selecting all SNPs variants without taking into account their SIFT scores.


This workflow is based on the application described in: https://github.com/rosafilgueira/Mutation_Sets

## Prerequisites

This workflow is fully compatible with Pegasus WMS, we have the following prerequisites:

- **Pegasus** - version 5.0 or higher
- **Python** - version 3.6 or higher
- **HTCondor** - version 9.0 or higher

Note that, there exists a version of this workflow comaptible with Pegasus 4.9 under the branch `4.9`.

## Running this worklow

Unzipping Input Data
---------------------
```
./prepare_input.sh
```

Creating a Workflow
---------------------
```
./daxgen.py
```
Or,
```
./daxgen.py -d 1000genome.dax -D 20130502 -f data.csv
```
This workflow assumes that all input data listed in the `data.csv` file is available in the `data/20130502` folder by default (but you can change that behavior with the `-D`).

Submitting a Workflow
---------------------

### HTCcondor

By default this workflow will run on a local available HTCondor pool, you have nothing to set.

### HPC clusters at NERSC
You can submit this workflow at The National Energy Research Scientific Computing Center (NERSC) on [Cori](https://docs.nersc.gov/systems/cori/) if you have an account there. You will havw to use Bosco to submit remotely.

#### Bosco

TODO

#### Submission mode

Then, when generating the workflow with `daxgen.py`, just set the flag `--execution-site cori` instead of the default `local` which will use an HTCondor pool.

#### Clustering mode
We have implement several clustering methods in that workflow, we cluster all the *individuals* and *individuals_merge* jobs together to improve performance.
By default there is no clustering used (for more information about clustering in Pegasus see [here](https://pegasus.isi.edu/documentation/user-guide/optimization.html?highlight=cluster#job-clustering)). 

However, interested users can use two different types of clustering that can improve performance:
 1. MPI: You can use [Pegasus MPI Cluster](https://pegasus.isi.edu/documentation/manpages/pegasus-mpi-cluster.html) mode  with the flag `--pmc`, which allow Pegasus to run multiple jobs inside using a classic leader and follower paradigm using MPI.
 2. MPI In-memory: You can also use an in-memory system called [Decaf](https://bitbucket.org/tpeterka1/decaf/) with the flag `--decaf` (_Warning_: these two options are mutually exclusive!) 

