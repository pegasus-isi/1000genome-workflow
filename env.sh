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
DECAF_LIBDIR=$(dirname "$(readlink -f "${BASH_SOURCE[-1]}")")
DECAF_LIBDIR="${DECAF_LIBDIR}/lib"
export LD_LIBRARY_PATH="${DECAF_LIBDIR}:${LD_LIBRARY_PATH}"
export PYTHONPATH="${DECAF_LIBDIR}:${PYTHONPATH}"

