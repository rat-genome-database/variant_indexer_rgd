#!/usr/bin/env bash

APPDIR=/home/rgddata/pipelines/VariantIndexer
VCF_DIR=/data/VCF_Human
COMMAND=update
PROCESS=$1
MAPKEY=$2

SPECIESTYPEKEY=1
ENV=dev
i=1
echo "Downloading VCF CHROMOSOME $i..."
wget -nv ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz -P $VCF_DIR
echo "******Downloaded chromosome $i*********"

#STEP-3: PROCESS VARIANTS
echo "STARTED processing variants of chromosome $i..."
echo "$APPDIR/run.sh  reindex $ENV $PROCESS $SPECIESTYPEKEY $MAPKEY $i $VCF_DIR/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
$APPDIR/run.sh  reindex $ENV $PROCESS $SPECIESTYPEKEY $MAPKEY $i $VCF_DIR/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
echo "********END processing variants of chromosome $i*******"

#STEP-4: DELETE VCF FILE
echo "deleting vcf file of chromosome $i...."
echo "rm $VCF_DIR/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
rm $VCF_DIR/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
echo "DONE INDEXING CHROMOSOME $i!!"

for i in {2..22..1}
do
#STEP-1: DOWNLOAD VCF FILE
echo "Downloading VCF CHROMOSOME $i..."
wget -nv ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz -P $VCF_DIR
echo "******Downloaded chromosome $i*********"

#STEP-3: PROCESS VARIANTS
echo "STARTED processing variants of chromosome $i..."
echo "$APPDIR/run.sh  $COMMAND $ENV $PROCESS $SPECIESTYPEKEY $MAPKEY $i $VCF_DIR/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
$APPDIR/run.sh  $COMMAND $ENV $PROCESS $SPECIESTYPEKEY $MAPKEY $i $VCF_DIR/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
echo "********END processing variants of chromosome $i*******"

#STEP-4: DELETE VCF FILE
echo "deleting vcf file of chromosome $i...."
echo "rm $VCF_DIR/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
rm $VCF_DIR/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
echo "DONE INDEXING CHROMOSOME $i!!"
done
date