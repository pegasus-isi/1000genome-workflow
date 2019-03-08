# 1000Genome Workflow

The 1000 genomes project provides a reference for human variation, having reconstructed the genomes of 2,504 individuals across 26 different populations to energize these approaches. This workflow identifies mutational overlaps using data from the 1000 genomes project in order to provide a null distribution for rigorous statistical evaluation of potential disease-related mutations. The workflow fetchs, parses, and analyzes data from the 1000 genomes project. It cross-matches the extracted data (which person has which mutations), with the mutation's sift score (how bad it is). Then it performs a few analyses, including plotting.

The figure below shows a branch of the workflow for the analysis of a single chromossome.

<img src="docs/images/1000genome.png?raw=true" width="450">

_Individuals_. This task fetches and parses the Phase3 data from the 1000 genomes project by chromosome. These files list all of Single nucleotide polymorphisms (SNPs) variants in that chromosome and which individuals have each one. SNPs are the most common type of genetic variation among people, and are the ones we consider in this work. An individual task creates output files for each individual of _rs numbers_ 3, where individuals have mutations on both alleles.

_Populations_. The 1000 genome project has 26 different populations from many different locations around the globe. A population task downloads a file per population selected. This workflow uses five super populations: African (AFR), Mixed American (AMR), East Asian (EAS), European (EUR), and South Asian (SAS). The workflow also uses ALL population, which means that all individuals from the latest release are considered.

_Sifting_. A sifting task computes the SIFT scores of all of the SNPs variants, as computed by the Variant Effect Predictor (VEP). SIFT is a sequence homology-based tool that Sorts Intolerant From Tolerant amino acid substitutions, and predicts whether an amino acid substitution in a protein will have a phenotypic effect. For each chromosome the sifting task processes the corresponding VEP, and selects only the SNPs variants that has a SIFT score, recording in a file (per chromosome) the SIFT score and the SNPs variants ids, which are: (1) rs number, (2) ENSEMBL GEN ID, and (3) HGNC ID.

_Pair Overlap Mutations_. This task measures the overlap in mutations (also called SNPs variants) among pairs of individuals by population and by chromosome.

_Frequency Overlap Mutations_. This tasks measures the frequency of overlapping in mutations by selecting a number of random individuals, and selecting all SNPs variants without taking into account their SIFT scores.


This workflow is based on the application described in: https://github.com/rosafilgueira/Mutation_Sets

## Generating and Running the Workflow

### Obtaining the example input data

- Creating the folders structure:
```
mkdir -p data/20130502/sifting
```

- Downloading the chromossome files:
```
cd data/20130502
wget ftp://anonymous@ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
wget ftp://anonymous@ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
gunzip ALL.*.gz
cd ../..
```

- Downloading the annotations files:
```
cd data/20130502/sifting
wget ftp://anonymous@ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/supporting/functional_annotation/filtered/ALL.chr21.phase3_shapeit2_mvncall_integrated_v5.20130502.sites.annotation.vcf.gz  
wget ftp://anonymous@ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/supporting/functional_annotation/filtered/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.sites.annotation.vcf.gz
gunzip ALL.*.gz
cd ../../..
```

### Generating a workflow DAX
```
./generate_dax.sh -d 1000genome.dax -D 20130502 -f data.csv
```

This workflow assumes that all input data listed in the `data.csv` file is available in the `data/20130502` folder, for example. To see the list of all options available, run `./generate_dax.sh -h`

Running a Workflow
-------------------
```
./plan_dax.sh 1000genome.dax
```
