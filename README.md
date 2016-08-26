# 1000Genome Workflow

The workflow fetchs, parses, and analyzes data from the 1000 genomes project (ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/). It cross-matches the extracted data (which person has which mutations), with the mutation's sift score (how bad it is). Then it performs a few analyses, including plotting.

<img src="docs/images/1000genome.png?raw=true" width="450">

Generating a Workflow
---------------------
```
./generate_dax.sh 1000genome.dax 20130502 data.csv
```

Running a Workflow
-------------------
```
./plan_dax.sh 1000genome.dax
```
