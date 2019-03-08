#!/usr/bin/env bash

workdir=`pwd`

cd $workdir/data/20130502
gunzip -k 'ALL.chr21.80000.vcf.gz'
gunzip -k 'ALL.chr22.80000.vcf.gz'

cd $workdir/data/20130502/sifting
gunzip -k 'ALL.chr21.phase3_shapeit2_mvncall_integrated_v5.20130502.sites.annotation.vcf.gz'
gunzip -k 'ALL.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.sites.annotation.vcf.gz'

cd $workdir
