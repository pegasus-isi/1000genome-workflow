#!/bin/bash
# Loaded Modules
module purge
unset LD_LIBRARY_PATH
module load PrgEnv-gnu
module load craype-haswell
module load cray-mpich
module load python
module load cmake
module load boost/1.69.0

# Decaf
#DECAF_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[-1]}")")
DECAF_DIR="/global/cfs/cdirs/m2187/pegasus-decaf/1000genome-workflow"
DECAF_LIBDIR="${DECAF_DIR}/lib"
export LD_LIBRARY_PATH="${DECAF_LIBDIR}:${LD_LIBRARY_PATH}"
export PYTHONPATH="${DECAF_LIBDIR}:${PYTHONPATH}"

