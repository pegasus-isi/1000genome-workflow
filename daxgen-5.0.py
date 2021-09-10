#!/usr/bin/env python3
from typing import Optional, Dict, List
from argparse import ArgumentParser
import sys
import logging
import os
import csv
from datetime import datetime
from pathlib import Path

from Pegasus.api import *

# logging.basicConfig(level=logging.DEBUG)

# --- Import Pegasus API ------------------------------------------------------

# API Documentation: http://pegasus.isi.edu/documentation

class GenomeWorkflow(object):
    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    dagfile = None
    wf_name = None
    wf_dir = None

    # --- Init ----------------------------------------------------------------
    def __init__(self,
                    datafile: str = 'data.csv',
                    dataset: str = '20130502',
                    ind_jobs: int = 250,
                    exec_site: Optional[str] = "condorpool",
                    use_bash: Optional[bool] = False,
                    columns: str = 'columns.txt',
                ) -> None:
        self.wf_name = "1000-genome"
        self.wid = self.wf_name + "-" + datetime.now().strftime("%s")
        self.dagfile = self.wid+".yml"

        self.wf_dir = str(Path(__file__).parent.resolve()) + '/'
        
        self.dataset = dataset
        self.datafile = datafile
        self.exec_site = exec_site
        self.columns = File(columns)
        self.ind_jobs = ind_jobs

        self.suffix = ".py"
        if use_bash:
            self.suffix = ""

        ## Output Sites
        self.shared_scratch_dir = os.path.join(
            self.wf_dir, "{}/scratch".format(self.wid))
        self.local_storage_dir = os.path.join(
            self.wf_dir, "{}/output".format(self.wid))


        # Population Files
        self.populations = []
        for base_file in os.listdir(self.wf_dir + 'data/populations/'):
          f_pop = File(base_file)
          self.populations.append(f_pop)


    # --- Write files in directory --------------------------------------------
    def write(self, produce_dot: bool = False):
        if not self.sc is None:
            self.sc.write()
        self.props.write()
        self.tc.write()
        self.rc.write()
        self.wf.write()

        if produce_dot:
            ## Produce a dot file of the workflow one can plot with dot -Tpdf graph.dot -o graph.pdf
            self.wf.graph(include_files=False, no_simplify=False, label="xform-id", output=self.wf_dir + '/' + self.wid + ".dot")


    # --- Configuration (Pegasus Properties) ----------------------------------
    def create_pegasus_properties(self):
        self.props = Properties()

    # --- Site Catalog --------------------------------------------------------
    def create_sites_catalog(self) -> None:
        self.sc = SiteCatalog()

        local = Site("local").add_directories(
            Directory(Directory.SHARED_SCRATCH, self.shared_scratch_dir).add_file_servers(
                FileServer("file://" + self.shared_scratch_dir, Operation.ALL)
            ),
            Directory(Directory.LOCAL_STORAGE, self.local_storage_dir).add_file_servers(
                FileServer("file://" + self.local_storage_dir, Operation.ALL)
            ),
        )

        condorpool = (
            Site("condorpool")
            .add_pegasus_profile(style="condor")
            .add_condor_profile(universe="vanilla")
            .add_profiles(Namespace.PEGASUS, key="data.configuration", value="condorio")
        )

        self.sc.add_sites(local, condorpool)

        if self.exec_site == "cori":
            cori = (
                Site("cori")
                .add_grids(
                    Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.SLURM,
                         contact="${NERSC_USER}@cori.nersc.gov", job_type=SupportedJobs.COMPUTE),
                    Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.SLURM,
                         contact="${NERSC_USER}@cori.nersc.gov", job_type=SupportedJobs.AUXILLARY)
                )
                .add_directories(
                    Directory(Directory.SHARED_SCRATCH,
                              "/global/cscratch1/sd/${NERSC_USER}/pegasus/scratch")
                    .add_file_servers(FileServer("file:///global/cscratch1/sd/${NERSC_USER}/pegasus/scratch", Operation.ALL)),
                    Directory(Directory.SHARED_STORAGE,
                              "/global/cscratch1/sd/${NERSC_USER}/pegasus/storage")
                    .add_file_servers(FileServer("file:///global/cscratch1/sd/${NERSC_USER}/pegasus/storage", Operation.ALL))
                )
                .add_pegasus_profile(
                    style="ssh",
                    data_configuration="sharedfs",
                    change_dir="true",
                    project="${NERSC_PROJECT}",
                    runtime="300"
                    # grid_start = "NoGridStart"
                )
                .add_env(key="PEGASUS_HOME", value="${NERSC_PEGASUS_HOME}")
            )
            self.sc.add_sites(cori)

    # --- Transformation Catalog (Executables and Containers) -----------------

    def create_transformation_catalog(self) -> None:
        self.tc = TransformationCatalog()

        e_individuals = (
            Transformation(
                "individuals",
                site="local",
                pfn=self.wf_dir + '/bin/individuals' + self.suffix,
                is_stageable=True,
            ).add_profiles(Namespace.PEGASUS, key="label", value="decaf")
        )

        e_individuals_merge = (
            Transformation(
                "individuals_merge",
                site="local",
                pfn=self.wf_dir + '/bin/individuals_merge' + self.suffix,
                is_stageable=True,
            ).add_profiles(Namespace.PEGASUS, key="label", value="decaf")
        )

        e_sifting = Transformation(
            "sifting",
            site="local",
            pfn=self.wf_dir + '/bin/sifting' + self.suffix,
            is_stageable=True,
        )

        e_mutation_overlap = Transformation(
            "mutation_overlap",
            site="local",
            pfn=self.wf_dir + '/bin/mutation_overlap' + self.suffix,
            is_stageable=True,
        )

        e_freq = Transformation(
            "frequency",
            site="local",
            pfn=self.wf_dir + '/bin/frequency.py',
            is_stageable=True,
        )

        if self.exec_site == "cori":
            pegasus_transfer = (
                Transformation("transfer", namespace="pegasus", site="cori",
                               pfn="$PEGASUS_HOME/bin/pegasus-transfer", is_stageable=False)
                .add_pegasus_profile(
                    queue="@escori",
                    runtime="300",
                    glite_arguments="--qos=xfer --licenses=SCRATCH"
                )
                .add_profiles(Namespace.PEGASUS, key="transfer.threads", value="8")
                .add_env(key="PEGASUS_TRANSFER_THREADS", value="8")
            )
            pegasus_dirmanager = (
                Transformation("dirmanager", namespace="pegasus", site="cori",
                               pfn="$PEGASUS_HOME/bin/pegasus-transfer", is_stageable=False)
                .add_pegasus_profile(
                    queue="@escori",
                    runtime="300",
                    glite_arguments="--qos=xfer --licenses=SCRATCH"
                )
            )
            pegasus_cleanup = (
                Transformation("cleanup", namespace="pegasus", site="cori",
                               pfn="$PEGASUS_HOME/bin/pegasus-transfer", is_stageable=False)
                .add_pegasus_profile(
                    queue="@escori",
                    runtime="60",
                    glite_arguments="--qos=xfer --licenses=SCRATCH"
                )
            )
            system_chmod = (
                Transformation("chmod", namespace="system", site="cori",
                               pfn="/usr/bin/chmod", is_stageable=False)
                .add_pegasus_profile(
                    queue="@escori",
                    runtime="120",
                    glite_arguments="--qos=xfer --licenses=SCRATCH"
                )
            )

            # github_location = "https://raw.githubusercontent.com/pegasus-isi/io-synthetic/master"
            # wrapper_fn = os.path.join(
            #     github_location, "bin/wrapper_darshan.sh")
            # keg = (
            #     Transformation("keg", site="cori",
            #                    pfn=wrapper_fn, is_stageable=True)
            #     .add_pegasus_profile(
            #         cores="1",
            #         runtime="1800",
            #         glite_arguments="--qos=debug --constraint=haswell --licenses=SCRATCH",
            #     )
            #     .add_env(key="USER_HOME", value="${NERSC_USER_HOME}")
            # )

            self.tc.add_transformations(pegasus_transfer, pegasus_dirmanager, pegasus_cleanup, system_chmod)

        self.tc.add_transformations(
            e_individuals, e_individuals_merge, e_sifting, e_mutation_overlap, e_freq)

    # --- Replica Catalog -----------------

    def create_replica_catalog(self) -> None:
        self.rc = ReplicaCatalog()

        file_site = "local"
        if self.exec_site == "cori":
            file_site = "cori"

        self.rc.add_replica(site=file_site, lfn=self.columns,
                            pfn=self.wf_dir + 'data/' + self.columns.lfn)

        for popfile in self.populations:
            self.rc.add_replica(site=file_site, lfn=popfile,
                                pfn=self.wf_dir + 'data/populations/' + popfile.lfn)

    # --- Create Workflow -----------------------------------------------------

    def create_workflow(self) -> None:
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        c_nums = []
        individuals_files = []
        sifted_files = []
        sifted_jobs = []
        individuals_merge_jobs = []
        
        with open(self.datafile, 'r') as f:
            for row in csv.reader(f):
                base_file = row[0]
                threshold = int(row[1])
                # To ensure we do not create too many individuals jobs
                ind_jobs = min(self.ind_jobs, threshold)
                step = threshold // ind_jobs
                rest = threshold % ind_jobs
                if rest != 0:
                    sys.exit("ERROR: for file {}: required individuals jobs {} does not divide the number of rows {}.".format(
                        base_file, ind_jobs, threshold))

                counter = 1

                individuals_jobs = []
                output_files = []

                # Individuals Jobs
                f_individuals = File(base_file)
                self.rc.add_replica(site=self.exec_site, lfn=f_individuals, pfn=self.wf_dir +
                                    '/data/' + self.dataset + '/' + f_individuals.lfn)

                c_num = base_file[base_file.find('chr')+3:]
                c_num = c_num[0:c_num.find('.')]
                c_nums.append(c_num)

                while counter < threshold:
                    stop = counter + step

                    out_name = 'chr%sn-%s-%s.tar.gz' % (c_num, counter, stop)
                    output_files.append(out_name)
                    f_chrn = File(out_name)

                    j_individuals = (
                        Job('individuals')
                            .add_args(f_individuals, c_num, str(counter), str(stop), str(threshold))
                            .add_inputs(f_individuals, self.columns)
                            .add_outputs(f_chrn, stage_out=False, register_replica=False)
                    )

                    individuals_jobs.append(j_individuals)
                    self.wf.add_jobs(j_individuals)

                    counter = counter + step

                # merge job
                j_individuals_merge = Job('individuals_merge').add_args(c_num)

                for out_name in output_files:
                    f_chrn = File(out_name)
                    j_individuals_merge.add_inputs(f_chrn)
                    j_individuals_merge.add_args(f_chrn)

                individuals_filename = 'chr%sn.tar.gz' % c_num
                f_chrn_merged = File(individuals_filename)
                individuals_files.append(f_chrn_merged)
                j_individuals_merge.add_outputs(f_chrn_merged, stage_out=False, register_replica=False)

                self.wf.add_jobs(j_individuals_merge)
                individuals_merge_jobs.append(j_individuals_merge)

                # Sifting Job
                f_sifting = File(row[2])
                self.rc.add_replica(site=self.exec_site, lfn=f_sifting, pfn=self.wf_dir +
                                    '/data/' + self.dataset + '/sifting/' + f_sifting.lfn)

                f_sifted = File('sifted.SIFT.chr%s.txt' % c_num)
                sifted_files.append(f_sifted)

                j_sifting = (
                    Job('sifting')
                        .add_inputs(f_sifting)
                        .add_outputs(f_sifted, stage_out=False, register_replica=False)
                        .add_args(f_sifting, c_num)
                )

                self.wf.add_jobs(j_sifting)
                sifted_jobs.append(j_sifting)

            # Analyses jobs
            for i in range(len(individuals_files)):
                for f_pop in self.populations:
                    # Mutation Overlap Job
                    f_mut_out = File('chr%s-%s.tar.gz' % (c_nums[i], f_pop.lfn))
                    j_mutation = (
                        Job('mutation_overlap')
                            .add_args('-c', c_nums[i], '-pop', f_pop)
                            .add_inputs(individuals_files[i], sifted_files[i], f_pop, self.columns)
                            .add_outputs(f_mut_out, stage_out=True, register_replica=False)
                    )
                    # Frequency Mutations Overlap Job
                    f_freq_out = File('chr%s-%s-freq.tar.gz' % (c_nums[i], f_pop.lfn))
                    j_freq = (
                        Job('frequency')
                            .add_args('-c', c_nums[i], '-pop', f_pop)
                            .add_inputs(individuals_files[i], sifted_files[i], f_pop, self.columns)
                            .add_outputs(f_freq_out, stage_out=True, register_replica=False)
                    )
                    self.wf.add_jobs(j_mutation, j_freq)


    # --- Run Workflow -----------------------------------------------------

    def run(self, submit=False, wait=False):
        try:
            plan_site = [self.exec_site]
            self.wf.plan(
                dir=self.wf_dir,
                relative_dir=self.wid,
                sites=plan_site,
                output_sites=["local"],
                output_dir=self.local_storage_dir,
                cleanup="leaf",
                force=True,
                submit=submit,
                verbose=0
            )
            if wait:
                self.wf.wait()

        except Exception as e:
            print(e)
            sys.exit(-1)


if __name__ == "__main__":
    parser = ArgumentParser(description="Pegasus 1000Genome Workflow")

    parser.add_argument(
        "-s",
        "--skip-sites-catalog",
        action="store_true",
        help="Skip site catalog creation",
    )

    parser.add_argument(
        "-e",
        "--execution-site",
        metavar="STR",
        type=str,
        default="condorpool",
        help="Execution site name (default: condorpool)",
    )

    parser.add_argument(
        '-D', 
        '--dataset', 
        action='store', 
        dest='dataset', 
        default='20130502', 
        help='Dataset folder'
    )

    parser.add_argument(
        '-f', 
        '--datafile', 
        action='store', 
        dest='datafile', 
        default='data.csv', 
        help='Data file with list of input data'
    )

    parser.add_argument(
        '-b', 
        '--bash-jobs', 
        action='store_true', 
        dest='use_bash', 
        help='Use original bash scripts for individuals, individuals_merge and sifting'
    )

    parser.add_argument(
        '-i',
        '--individuals-jobs',
        action='store',
        dest='ind_jobs',
        default=250,
        type=int,
        help='Number of individuals jobs that will be created for each chromosome \
            (if larger than the total number of rows in the data for that chromosome, \
            then it will be set to the number of rows so each job will process one row)'
    )

    args = parser.parse_args()

    workflow = GenomeWorkflow(
        datafile = args.datafile,
        dataset = args.dataset,
        ind_jobs = args.ind_jobs,
        exec_site=args.execution_site,
        use_bash = args.use_bash
    )

    if not args.skip_sites_catalog:
        print("Creating execution sites...")
        workflow.create_sites_catalog()

    print("Creating workflow properties...")
    workflow.create_pegasus_properties()

    print("Creating transformation catalog...")
    workflow.create_transformation_catalog()

    print("Creating replica catalog...")
    workflow.create_replica_catalog()

    print("Creating pipeline workflow dag...")
    workflow.create_workflow()

    workflow.write(produce_dot=True)
    workflow.run(submit=False, wait=False)
