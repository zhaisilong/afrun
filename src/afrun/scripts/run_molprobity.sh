#!/bin/bash

set -e

WORKDIR=$1
PROTEIN_PATH=$2
PREFIX=$3
SINGULARITY_PATH="/data/home/silong/projects/peptide/PepPCBench/tools/MolProbity/molprobity.sif"
pushd $WORKDIR

echo "$WORKDIR Extracting peptide and protein pockets..."
singularity exec -e $SINGULARITY_PATH molprobity.molprobity $PROTEIN_PATH --prefix=$PREFIX

touch molprobity.done
popd
