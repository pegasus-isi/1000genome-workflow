# 1000Genomes Workflow

The 1000 genomes project provides a reference for human variation, having reconstructed the genomes of 2,504 individuals across 26 different populations to energize these approaches. This workflow identifies mutational overlaps using data from the 1000 genomes project in order to provide a null distribution for rigorous statistical evaluation of potential disease-related mutations. The workflow fetchs, parses, and analyzes data from the [1000 genomes Project](https://www.internationalgenome.org) (see ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/). It cross-matches the extracted data (which person has which mutations), with the mutation's sift score (how bad it is). Then it performs a few analyses, including plotting.

The figure below shows a branch of the workflow for the analysis of a single chromosome.

<p align="center">
  <img src="docs/images/1000genome.png?raw=true" width="600">
</p>

_Individuals_. This task fetches and parses the Phase3 data from the 1000 genomes project by chromosome. These files list all of Single nucleotide polymorphisms (SNPs) variants in that chromosome and which individuals have each one. SNPs are the most common type of genetic variation among people, and are the ones we consider in this work. An individual task creates output files for each individual of _rs numbers_ 3, where individuals have mutations on both alleles.

_Populations_. The 1000 genome project has 26 different populations from many different locations around the globe. A population task downloads a file per population selected. This workflow uses six super populations: African (AFR), Mixed American (AMR), East Asian (EAS), European (EUR), British from England and Scotland (GBR) and South Asian (SAS). The workflow also uses ALL population, which means that all individuals from the latest release are considered.

_Sifting_. A sifting task computes the SIFT scores of all of the SNPs variants, as computed by the Variant Effect Predictor (VEP). SIFT is a sequence homology-based tool that Sorts Intolerant From Tolerant amino acid substitutions, and predicts whether an amino acid substitution in a protein will have a phenotypic effect. For each chromosome the sifting task processes the corresponding VEP, and selects only the SNPs variants that has a SIFT score, recording in a file (per chromosome) the SIFT score and the SNPs variants ids, which are: (1) rs number, (2) ENSEMBL GEN ID, and (3) HGNC ID.

_Mutations_Overlap_. This task measures the overlap in mutations (also called SNPs variants) among pairs of individuals by population and by chromosome.

_Frequency_. This tasks measures the frequency of overlapping in mutations by selecting a number of random individuals, and selecting all SNPs variants without taking into account their SIFT scores.


This workflow is based on the application described in: https://github.com/rosafilgueira/Mutation_Sets

## Prerequisites

This workflow is fully compatible with Pegasus WMS, we have the following prerequisites:

- **Pegasus** - version 5.0 or higher
- **Python** - version 3.6 or higher
- **HTCondor** - version 9.0 or higher

Note that, there exists a version of this workflow compatible with Pegasus 4.9 under the branch `4.9`.

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
./daxgen.py -D 20130502 -f data.csv -i 1
```
This workflow assumes that all input data listed in the `data.csv` file is available in the `data/20130502` folder by default (but you can change that behavior with the `-D`).

### Workflow parallelism
You can control how many `individuals` jobs **per chromosome** will get created with the parameter `-i IND_JOBS, --individuals-jobs IND_JOBS`, by default it's set to `1`. If the value provided is larger than the total number of rows in the data file for that chromosome, then it will be set to the number of rows so that each job will process one row (_Warning_: this will extremely inefficient and will create a large number of jobs, about `250,000`).

In addition, it is required that `IND_JOBS` **divides the number of rows** for each chromosome, in this case `250,000`.

Submitting a Workflow
---------------------

### HTCcondor

By default this workflow will run on a [local](https://pegasus.isi.edu/documentation/user-guide/execution-environments.html#localhost) available HTCondor pool, you have nothing to set.

#### Execution times

This execution has 10 *individuals* jobs and 1 chromosome, it has been executed on one node of Cori at NERSC (Haswell). If there are multiple jobs from the same class (e.g., _Individuals_) that runs in parallel then we take the maximum duration among those. _Auxiliary_ represents internal jobs managed by Pegasus (create directory, chmod and cleanup jobs).

| **Job**           | **Duration (s)** | **Fraction (%)** |
|:------------------|-----------------:|-----------------:|
| Individuals       |            11431 |            81.85 |
| Frequency         |             1492 |            10.68 |
| Individuals_Merge |              500 |             3.58 |
| Mutation_Overlap  |              468 |             3.35 |
| Stage_Out         |               34 |             0.24 |
| Stage_In          |               21 |             0.15 |
| Auxiliary         |               16 |             0.11 |
| Sifting           |                6 |             0.05 |
|                   |                  |                  |
| **Total**         |     13967 (3.9h) |              100 |

#### Memory requirements

We discuss here some memory requirement for the *individuals* jobs which are by far the largest jobs of the workflow. This workflow processes a given number of chromosomes named `ALL.chrX.250000.vcf` where `X` is the number of the chromosome and `250000` is the number of lines of that file. If the workflow processes 10 chromosomes then we will have 10 *individuals* jobs and one *individuals_merge* job. However, because this file is extremely long (250k lines), we can create multiple individuals job to process one chromosome, then *individuals_merge* job will make sure we merge each chunk processed in parallel. For example if we create 5 *individuals* jobs per chromosome then eachjob will process only 50,000 lines instead of 250,000. If we have 10 chromosomes then we will have `10*5`  *individuals* jobs and `5`  *individuals_merge* jobs. 

| Total number of *individuals* job per chromosome | Input size per *individuals* job (number of lines) | Memory required per *individuals* job |
| :---                                             |    :----:                                          |                                  ---: |
| 2                                                | 125,000 / 250,000                                  | 6.10 GB                               |
| 5                                                | 50,000 / 250,000                                   | 3.93 GB                               |
| 10                                               | 25,000 / 250,000                                   | 3.17 GB                               |
| 16                                               | 15,625 / 250,000                                   | 2.93 GB                               |

>Tips: You can use `--individuals-jobs` or `-i` to to vary the number of *individuals* jobs per chromosomes (by default there is one *individuals* per chromosomes).

#### Insufficient memory for HTCondor slots

If HTCondor does not have enough memory to execute these jobs, Condor will send a SIGKILL (code -9) to the jobs. To avoid that you can configure HTCondor to use dynamic slot allocation (slots can grow on demand) with this (don't forget to restart Condor services after):

```
# dynamic slots
SLOT_TYPE_1 = cpus=100%,disk=100%,swap=100%
SLOT_TYPE_1_PARTITIONABLE = TRUE
NUM_SLOTS = 1
NUM_SLOTS_TYPE_1 = 1
```

### HPC clusters at NERSC
You can submit this workflow at The National Energy Research Scientific Computing Center (NERSC) on [Cori](https://docs.nersc.gov/systems/cori/) if you have an account there. You will have to use Bosco to submit remotely.

#### Bosco

We use [Bosco](https://osg-bosco.github.io/docs/) to enable remote submission to Cori at NERSC using SSH. For the ease of implementation, we utilize the Docker container [pegasus-nersc-bosco](https://github.com/pegasus-isi/pegasus-nersc-bosco).

#### Submission mode

Then inside the container, when generating the workflow with `daxgen.py`, just set the flag `--execution-site cori` instead of the default `local` which will use an HTCondor pool.

>Tips: You can clone this repository and prepare input data in a certain location on Cori, then specify `--src-path` to such the directory. By doing that, it is able to avoid the overhead of staging large amount of data.

#### Clustering mode
We have implement several clustering methods in that workflow, we cluster all the *individuals* and *individuals_merge* jobs together to improve performance.
By default there is no clustering used (for more information about clustering in Pegasus see [here](https://pegasus.isi.edu/documentation/user-guide/optimization.html?highlight=cluster#job-clustering)). 

However, interested users can use two different types of clustering that can improve performance:
 1. MPI: You can use [Pegasus MPI Cluster](https://pegasus.isi.edu/documentation/manpages/pegasus-mpi-cluster.html) mode  with the flag `--pmc`, which allow Pegasus to run multiple jobs inside using a classic leader and follower paradigm using MPI.
 2. MPI In-memory: You can also use an in-memory system called [Decaf](https://bitbucket.org/tpeterka1/decaf/) [1] with the flag `--decaf` (_Warning_: these two options are mutually exclusive!) 

# References

Dreher, Matthieu, and Tom Peterka. _Decaf: Decoupled dataflows for in situ high-performance workflows._ No. ANL/MCS-TM-371. Argonne National Lab.(ANL), Argonne, IL (United States), 2017. https://www.mcs.anl.gov/~tpeterka/papers/2017/dreher-anl17-report.pdf
