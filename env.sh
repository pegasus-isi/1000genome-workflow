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
#export DECAF_PREFIX="${HOME}/software/install/decaf"
DECAF_LIBDIR="/global/cfs/cdirs/m2187/pegasus-decaf/1000genome-workflow/lib"
export LD_LIBRARY_PATH="${DECAF_LIBDIR}:${LD_LIBRARY_PATH}"
export PYTHONPATH="${DECAF_LIBDIR}:${PYTHONPATH}"

