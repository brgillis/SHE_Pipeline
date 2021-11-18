""" @file run_bias_pipeline_parallel.py

    Created Aug 2018

    Main executable for running bias pipeline in parallel
"""

__updated__ = "2021-08-18"

# Copyright (C) 2012-2020 Euclid Science Ground Segment
#
# This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation; either version 3.0 of the License, or (at your option)
# any later version.
#
# This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along with this library; if not, write to
# the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

import math
import multiprocessing
import os
from collections import namedtuple
from pickle import UnpicklingError
from xml.sax import SAXParseException

import SHE_CTE_BiasMeasurement.MeasureBias as meas_bias
import SHE_CTE_BiasMeasurement.MeasureStatistics as meas_stats
import SHE_CTE_PipelineUtility.CleanupBiasMeasurement as cleanup_bias
import SHE_CTE_ShearEstimation.EstimateShear as est_she
import SHE_GST_GalaxyImageGeneration.GenGalaxyImages as gen_galimg
import SHE_GST_PrepareConfigs.write_configs as gst_prep_conf
import SHE_GST_cIceBRGpy
import SHE_Pipeline
from SHE_CTE_BiasMeasurement.measure_bias import measure_bias_from_args
from SHE_CTE_BiasMeasurement.measure_statistics import measure_statistics_from_args
from SHE_CTE_ShearEstimation.estimate_shears import estimate_shears_from_args
from SHE_GST_GalaxyImageGeneration.generate_images import generate_images
from SHE_GST_GalaxyImageGeneration.run_from_config import run_from_args
from SHE_PPT.file_io import (find_file, get_allowed_filename, read_listfile, read_xml_product, write_listfile)
from SHE_PPT.logging import getLogger
from SHE_PPT.mdb import Mdb, mdb_keys
from SHE_PPT.pipeline_utility import CalibrationConfigKeys
from . import pipeline_utilities as pu, run_pipeline as rp
from .constants import ERun_CTE, ERun_GST
from .pipeline_info import pipeline_info_dict
from .pipeline_utilities import get_relpath

MSG_EXEC_FINISHED_SUCCESS = "Finished command execution successfully."

MSG_EXEC_FAILED = "Execution failed with error: "

default_workdir = "/home/user/Work/workspace"
default_logdir = "logs"
default_cluster_workdir = "/workspace/lodeen/workdir"

non_filename_args = ("workdir", "logdir", "pkgRepository", "pipelineDir")

logger = getLogger(__name__)


def she_prepare_configs(simulation_plan, config_template,
                        simulation_configs, workdir):
    """ Runs SHE_GST Prepare configurations
    Sets up simulations using simulation plan and configuration
    template.
    Creates *cache.bin files
    """

    gst_prep_conf.write_configs_from_plan(
        plan_filename = get_relpath(simulation_plan, workdir),
        template_filename = get_relpath(config_template, workdir),
        listfile_filename = get_relpath(simulation_configs, workdir),
        workdir = workdir)
    logger.info("Prepared configurations")


def she_simulate_images(config_files, pipeline_config, data_images,
                        stacked_data_image, psf_images_and_tables, segmentation_images,
                        stacked_segmentation_image, detections_tables, details_table,
                        workdir, logdir, sim_number):
    """ Runs SHE_GST_GenGalaxyImages code, creating images, segmentations
    catalogues etc.
    """

    SHE_GST_cIceBRGpy.set_workdir(workdir)
    # @TODO: Replace with function call, see issue 11
    argv = ("--config_files %s "
            "--pipeline_config %s --data_images %s --stacked_data_image %s "
            "--psf_images_and_tables %s --segmentation_images %s "
            "--stacked_segmentation_image %s --detections_tables %s "
            "--details_table %s --workdir %s "
            "--log-file %s/%s/she_simulate_images.out"
            % (get_relpath(config_files, workdir),
               get_relpath(pipeline_config, workdir),
               get_relpath(data_images, workdir),
               get_relpath(stacked_data_image, workdir),
               get_relpath(psf_images_and_tables, workdir),
               get_relpath(segmentation_images, workdir),
               get_relpath(stacked_segmentation_image, workdir),
               get_relpath(detections_tables, workdir),
               get_relpath(details_table, workdir),
               workdir, workdir, logdir)).split()

    gen_gi_args = pu.setup_function_args(argv, gen_galimg, ERun_GST + "SHE_GST_GenGalaxyImages")

    # warnings out put as stdOut/stdErr --> send to log file..
    # Why is it not E-Run.err??
    try:
        run_from_args(generate_images, gen_gi_args)
    except Exception as e:
        logger.error(MSG_EXEC_FAILED + str(e))
        raise
    logger.info(MSG_EXEC_FINISHED_SUCCESS)


def she_estimate_shear(data_images, stacked_image,
                       psf_images_and_tables, segmentation_images,
                       stacked_segmentation_image, detections_tables,
                       ksb_training_data,
                       lensmc_training_data, momentsml_training_data,
                       regauss_training_data, pipeline_config, mdb,
                       shear_estimates_product, she_lensmc_chains,
                       workdir, logdir, sim_number):
    """ Runs the SHE_CTE_EstimateShear method that calculates
    the shear using 4 methods: KSB, LensMC, MomentsML and REGAUSS

    @todo: use defined options for which Methods to use...
    # It is in the pipeline config file...
    # Do checks for consistency (earlier)
    """

    # @TODO: Replace with function call, see issue 11

    # Check to see if training data exists.
    # @TODO: Simplify, avoid repetitions
    shear_method_arg_string = ""
    if ksb_training_data and ksb_training_data != 'None':
        shear_method_arg_string += " --ksb_training_data %s" % get_relpath(
            ksb_training_data, workdir)
    if lensmc_training_data and lensmc_training_data != 'None':
        shear_method_arg_string += " --lensmc_training_data %s" % get_relpath(
            lensmc_training_data, workdir)
    if momentsml_training_data and momentsml_training_data != 'None':
        shear_method_arg_string += " --momentsml_training_data %s" % get_relpath(
            momentsml_training_data, workdir)
    if regauss_training_data and regauss_training_data != 'None':
        shear_method_arg_string += " --regauss_training_data %s" % get_relpath(
            regauss_training_data, workdir)

    # @FIXME: --logdir is a pipeline runner option, not a shear_estimate option
    # shear_estimate etc. use magic values for the logger..

    argv = ("--data_images %s "
            "--stacked_image %s --psf_images_and_tables %s "
            "--segmentation_images %s --stacked_segmentation_image %s "
            "--detections_tables %s%s --pipeline_config %s --mdb %s "
            "--shear_estimates_product %s --she_lensmc_chains %s --workdir %s --logdir %s "
            "--log-file %s/%s/she_estimate_shear.out" %
            (get_relpath(data_images, workdir),
             get_relpath(stacked_image, workdir),
             get_relpath(psf_images_and_tables, workdir),
             get_relpath(segmentation_images, workdir),
             get_relpath(stacked_segmentation_image, workdir),
             get_relpath(detections_tables, workdir),
             shear_method_arg_string,
             get_relpath(pipeline_config, workdir),
             get_relpath(mdb, workdir),
             get_relpath(shear_estimates_product, workdir),
             get_relpath(she_lensmc_chains, workdir),
             workdir, logdir, workdir, logdir)).split()
    #

    # set argsparser

    estshr_args = pu.setup_function_args(argv, est_she, ERun_CTE + " SHE_CTE_EstimateShear")

    try:
        estimate_shears_from_args(estshr_args)
    except Exception as e:
        logger.error("Execution failed with error: " + str(e))
        raise
    logger.info("Finished command execution successfully.")


def she_measure_statistics(details_table, shear_estimates,
                           pipeline_config, she_bias_statistics, workdir, logdir):
    """ Runs the SHE_CTE_MeasureStatistics method on shear
    estimates to get shear bias statistics.
    """

    argv = ("--details_table %s "
            "--shear_estimates %s --pipeline_config %s "
            "--she_bias_statistics %s "
            "--workdir %s "
            "--log-file %s/%s/she_measure_statistics.out "
            "--bins_description %s"
            % (get_relpath(details_table, workdir),
               get_relpath(shear_estimates, workdir),
               get_relpath(pipeline_config, workdir),
               get_relpath(she_bias_statistics, workdir),
               workdir, workdir, logdir,
               get_relpath(bins_description,workdir))).split()

    measure_stats_args = pu.setup_function_args(argv, meas_stats,
                                                ERun_CTE + "SHE_CTE_MeasureStatistics")

    try:
        measure_statistics_from_args(measure_stats_args)

    except Exception as e:
        logger.error("Execution failed with error: " + str(e))
        raise
    logger.info("Finished command execution successfully.")


def she_cleanup_bias_measurement(simulation_config, data_images,
                                 stacked_data_image, psf_images_and_tables, segmentation_images,
                                 stacked_segmentation_image, detections_tables, details_table,
                                 shear_estimates, shear_bias_statistics_in, pipeline_config,
                                 she_bias_measurements, workdir, logdir, sim_number):
    """ Runs the SHE_CTE_CleanupBiasMeasurement code on she_bias_statistics.
    Returns she_bias_measurements
    """

    argv = ("--simulation_config %s "
            "--data_images %s --stacked_data_image %s --psf_images_and_tables %s "
            "--segmentation_images %s --stacked_segmentation_image %s "
            "--detections_tables %s --details_table %s --shear_estimates %s "
            "--shear_bias_statistics_in %s --pipeline_config %s "
            "--shear_bias_statistics_out %s --workdir %s "
            "--log-file %s/%s/she_cleanup_bias_measurement.out" % (
                get_relpath(simulation_config, workdir),
                get_relpath(data_images, workdir),
                get_relpath(stacked_data_image, workdir),
                get_relpath(psf_images_and_tables, workdir),
                get_relpath(segmentation_images, workdir),
                get_relpath(stacked_segmentation_image, workdir),
                get_relpath(detections_tables, workdir),
                get_relpath(details_table, workdir),
                get_relpath(shear_estimates, workdir),
                get_relpath(shear_bias_statistics_in, workdir),
                get_relpath(pipeline_config, workdir),
                get_relpath(she_bias_measurements, workdir),
                workdir, workdir, logdir)).split()

    cleanbias_args = pu.setup_function_args(argv, cleanup_bias, ERun_CTE + "SHE_CTE_CleanupBiasMeasurement")
    try:
        cleanup_bias.cleanup_bias_measurement_from_args(cleanbias_args)
    except Exception as e:
        logger.error("Execution failed with error: " + str(e))
        raise
    logger.info("Finished command execution successfully")


def she_measure_bias(shear_bias_measurement_list, pipeline_config,
                     shear_bias_measurement_final, bins_description, workdir, logdir):
    """ Runs the SHE_CTE_MeasureBias on a list of she_bias_measurements from
    all simulation runs.
    """

    argv = ("--she_bias_statistics %s "
            "--pipeline_config %s --she_bias_measurements %s --workdir %s "
            "--log-file %s/%s/she_measure_bias.out "
            "--bins_description %s"
            % (get_relpath(shear_bias_measurement_list, workdir),
               get_relpath(pipeline_config, workdir),
               get_relpath(shear_bias_measurement_final, workdir),
               workdir, workdir, logdir,
               get_relpath(bins_description,workdir))).split()

    measure_bias_args = pu.setup_function_args(argv, meas_bias, ERun_CTE + "SHE_CTE_MeasureBias")
    try:
        measure_bias_from_args(measure_bias_args)
    except Exception as e:
        logger.error("Execution failed with error: " + str(e))
        raise
    logger.info("Finished command execution successfully")


def check_args(args):
    """Checks arguments for validity and fixes if possible.
    Modified from similar function in run_pipeline

    One big modification is that it checks that number of threads
    has been set and if not uses multiprocessing to do so
    and then sets up the multiple directory structure for each
    thread.

    @return dirStruct
    @rType  list(namedtuple)
    """

    logger.debug('# Entering SHE_Pipeline_RunBiasParallel check_args()')

    pipeline = 'bias_measurement'

    if pipeline not in pipeline_info_dict:
        err_string = ("Unknown pipeline specified to be run: " + pipeline + ". Allowed pipelines are: ")
        for allowed_pipeline in pipeline_info_dict:
            err_string += "\n  " + allowed_pipeline
        raise ValueError(err_string)
    chosen_pipeline_info = pipeline_info_dict[pipeline]

    # Does the pipeline we want to run exist?
    if not os.path.exists(chosen_pipeline_info.qualified_pipeline_script):
        logger.error("Pipeline '" + pipeline + "' cannot be found. Expected location: " +
                     chosen_pipeline_info.qualified_pipeline_script)

    # If no ISF is specified, use the default for this pipeline
    if args.isf is None:
        try:
            args.isf = chosen_pipeline_info.qualified_isf
        except Exception:
            logger.error("No ISF file specified, and cannot find one in default location (" +
                         chosen_pipeline_info.qualified_isf + "_isf.txt).")
            raise

    # If no config is specified, use the default for this pipeline
    if args.config is None:
        try:
            args.config = chosen_pipeline_info.qualified_config
        except Exception:
            logger.warning("No config file specified, and cannot find one in default location (" +
                           chosen_pipeline_info.qualified_config + "). Will run with no " +
                           "configuration parameters set.")

    # Check that we have an even number of ISF arguments
    if args.isf_args is None:
        args.isf_args = []
    if not len(args.isf_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'args': Must be a set of paired arguments.")

    # Check that we have an even number of pipeline_config arguments
    if args.config_args is None:
        args.config_args = []
    if not len(args.config_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'config_args': Must be a set of paired arguments.")

    # Check that all config args are recognized
    for i in range(len(args.config_args) // 2):
        test_arg = args.config_args[2 * i]
        if not CalibrationConfigKeys.is_allowed_value(test_arg):
            err_string = ("Config argument \"" + test_arg + "\" not recognized. Allowed arguments are: ")
            for allowed_key in CalibrationConfigKeys:
                err_string += "\n--" + allowed_key.value
            raise ValueError(err_string)

    # Use the default workdir if necessary
    if args.workdir is None:
        if args.cluster:
            args.workdir = default_cluster_workdir
        else:
            args.workdir = default_workdir
        logger.info('No workdir supplied at command-line. Using default workdir: ' + args.workdir)

    # Use the default logdir if necessary
    if args.logdir is None:
        args.logdir = default_logdir
        logger.info('No logdir supplied at command-line. Using default logdir: ' + args.logdir)
    qualified_logdir = os.path.join(args.workdir, args.logdir)

    if args.number_threads == 0:
        args.number_threads = str(max((multiprocessing.cpu_count() - 1, 1)))
    if not args.number_threads.isdigit():
        raise ValueError("Invalid values passed to 'number-threads': Must be an integer.")

    args.number_threads = max(1, min(int(args.number_threads), multiprocessing.cpu_count()))

    if args.est_shear_only:
        if not args.est_shear_only.isdigit() and int(args.est_shear_only) not in (0, 1):
            raise ValueError("Invalid value passes to est_shear_only must be 0,1")
        args.est_shear_only = int(args.est_shear_only) == 1

    # Create the base workdir
    if not os.path.exists(args.workdir):
        # Can we create it?
        try:
            os.mkdir(args.workdir)
        except Exception as e:
            logger.error(f"workdir ({args.workdir}) does not exist and cannot be created.")
            raise e
    if args.cluster:
        os.chmod(args.workdir, 0o777)  # Does the cache directory exist within the workdir?
    cache_dir = os.path.join(args.workdir, "cache")
    if not os.path.exists(cache_dir):
        # Can we create it?
        try:
            os.mkdir(cache_dir)
        except Exception as e:
            logger.error(f"Cache directory ({cache_dir}) does not exist and cannot be created.")
            raise e
    if args.cluster:
        os.chmod(cache_dir, 0o777)

    # Does the data directory exist within the workdir?
    data_dir = os.path.join(args.workdir, "data")
    if not os.path.exists(data_dir):
        # Can we create it?
        try:
            os.mkdir(data_dir)
        except Exception as e:
            logger.error(f"Data directory ({data_dir}) does not exist and cannot be created.")
            raise e
    if args.cluster:
        os.chmod(data_dir, 0o777)

    # Does the log directory exist within the workdir?
    log_dir = os.path.join(args.workdir, args.logdir)
    if not os.path.exists(log_dir):
        # Can we create it?
        try:
            os.mkdir(log_dir)
        except Exception as e:
            logger.error(f"Log directory ({log_dir}) does not exist and cannot be created.")
            raise e
    if args.cluster:
        os.chmod(log_dir, 0o777)

    if not os.path.exists(qualified_logdir):
        # Can we create it?
        try:
            os.mkdir(qualified_logdir)
        except Exception as e:
            logger.error(f"logdir ({qualified_logdir}) does not exist and cannot be created.")
            raise e
    if args.cluster:
        os.chmod(qualified_logdir, 0o777)

    # Check that plan args are formatted properly
    if args.plan_args is None:
        args.plan_args = []
    if not len(args.plan_args) % 2 == 0:
        raise ValueError("Invalid values passed to 'plan_args': Must be a set of paired arguments.")

    return chosen_pipeline_info


def get_dir_struct(args, num_batches):
    # @TODO: Be careful, workdir and app_workdir...
    # make sure number threads is valid
    # @FIXME: Check this...

    dir_struct = pu.create_thread_dir_struct(args, [args.workdir], int(args.number_threads), num_batches)

    return dir_struct


def create_batches(args, sim_config_list):
    """ Uses the simulation configuration list and the
    workdir (thread) list to split the simulations into
    batches.

    @return batchList
    @rType list(namedtuple)
    """
    # Overwrite values as necessary
    # @TODO: Do we do this multiple times and save multiple times - or add multiple lines?
    batch_tuple = namedtuple("Batch", "batch_number nThreads min_sim_number max_sim_number")

    sim_config_list = read_listfile(os.path.join(args.workdir, sim_config_list))
    number_simulations = len(sim_config_list)

    number_batches = math.ceil(number_simulations / args.number_threads)
    batch_list = []

    workdir_list = get_dir_struct(args, num_batches = number_batches)

    for batch_number in range(number_batches):

        num_threads = args.number_threads
        min_sim_number = args.number_threads * batch_number
        max_sim_number = args.number_threads * (batch_number + 1)
        if max_sim_number > number_simulations:
            num_threads = number_simulations - min_sim_number
            max_sim_number = number_simulations

        batch_list.append(batch_tuple(batch_number, num_threads,
                                      min_sim_number, max_sim_number))

    return batch_list, workdir_list


def get_sim_number(thread_number, batch):
    """ Returns simulation number calculated from thread_number and batch tuple
    @return: simulation_number
    @rType:  int
    """

    return batch.min_sim_number + thread_number


def create_simulate_measure_inputs(args, config_filename, workdir, sim_config_list,
                                   simulation_number):
    """Function to create a new ISF for this run by adjusting workdir and logdir, and overwriting any
       values passed at the command-line.

       More importantly, does symlinks to current thread to link correct
       files to different threads. Based on run_pipeline function

    @return: Simulation inputs
    @rtype:  namedtuple

    """

    inputs_tuple = namedtuple("SIMInputs", "simulation_config "
                              "ksb_training_data lensmc_training_data momentsml_training_data "
                              "regauss_training_data pipeline_config mdb bins_description")

    logger = getLogger(__name__)

    # Find the base ISF we'll be creating a modified copy of
    # @TODO: include batch_number in name
    base_isf = find_file(args.isf, path = args.workdir)
    new_isf_filename = get_allowed_filename("ISF",
                                            str(os.getpid()),
                                            extension = ".txt",
                                            version = SHE_Pipeline.__version__)
    qualified_isf_filename = os.path.join(workdir.workdir,
                                          new_isf_filename)

    # Set up the args we'll be replacing or setting

    args_to_set = {"workdir"        : workdir.workdir,
                   "logdir"         : workdir.logdir,
                   "pkgRepository"  : rp.get_pipeline_dir(),
                   "pipelineDir"    : rp.get_pipeline_dir(),
                   "pipeline_config": config_filename}

    arg_i = 0
    while arg_i < len(args.isf_args):

        args_to_set[args.isf_args[arg_i]] = args.isf_args[arg_i + 1]
        arg_i += 2

    with open(base_isf, 'r') as fi:
        # Check each line to see if values we'll overwrite are specified in it,
        # and only write out lines with other values
        for line in fi:
            split_line = line.strip().split('=')
            # Add any new args here to the list of args we want to set
            if not (split_line[0] in args_to_set) and len(split_line) > 1:
                args_to_set[split_line[0]] = split_line[1]

    simulation_config = read_listfile(os.path.join(
        args.workdir, sim_config_list))[simulation_number]
    args_to_set['simulation_config'] = simulation_config

    # Search path is root workdir
    search_path = args.workdir

    for input_port_name in args_to_set:

        # Skip ISF arguments that don't correspond to input ports
        if input_port_name in non_filename_args or 'simulation_plan' in input_port_name:
            continue

        filename = args_to_set[input_port_name]
        # Skip if None
        if filename is None or filename == "None":
            continue

        if 'TEST-%s' % simulation_number in filename:
            # File for this simulation
            pass
        elif 'TEST-' in filename:
            continue

        # Find the qualified location of the file
        try:
            qualified_filename = find_file(filename, path = search_path)
        except RuntimeError as _:
            raise RuntimeError("Input file " + filename + " cannot be found in path " + search_path)

        # Symlink the filename from the "data" directory within the workdir
        new_filename = os.path.join("data", os.path.split(filename)[1])
        try:
            if not qualified_filename == os.path.join(workdir.workdir, new_filename):
                os.symlink(qualified_filename, os.path.join(workdir.workdir, new_filename))
        except FileExistsError as _:
            try:
                os.remove(os.path.join(workdir.workdir, new_filename))
                try:
                    os.unlink(os.path.join(workdir.workdir, new_filename))
                except Exception as _:
                    pass
            except Exception as _:
                pass
            if not qualified_filename == os.path.join(workdir.workdir, new_filename):
                os.symlink(qualified_filename, os.path.join(workdir.workdir, new_filename))

        # Update the filename in the args_to_set to the new location
        args_to_set[input_port_name] = new_filename

        # Now, go through each data file of the product and symlink those from the workdir too

        data_filenames = []
        # Download MDB files if needed
        if input_port_name == "mdb":
            if filename[:4] == "WEB/":
                qualified_filename = find_file(filename)
                mdb_dict = Mdb(qualified_filename).get_all()
                web_mdb_path = os.path.split(filename)[0]
                for key in (mdb_keys.vis_gain_coeffs, mdb_keys.vis_readout_noise_table):
                    for data_filename in mdb_dict[key]['Value']:
                        web_data_filename = os.path.join(web_mdb_path, "data", data_filename)
                        find_file(web_data_filename)
                        data_filenames.append("data/" + data_filename)
        # Get all data files this product points to and symlink them to the main data dir
        elif qualified_filename[-4:] == ".xml":
            try:
                p = read_xml_product(qualified_filename)
                data_filenames = p.get_all_filenames()
            except (SAXParseException, UnpicklingError, UnicodeDecodeError) as _:
                logger.error("Cannot read file " + qualified_filename + ".")
                raise
        elif qualified_filename[-5:] == ".json":
            subfilenames = read_listfile(qualified_filename)
            for subfilename in subfilenames:
                qualified_subfilename = find_file(subfilename, path = search_path)
                try:
                    p = read_xml_product(qualified_subfilename)
                    data_filenames += p.get_all_filenames()
                except (SAXParseException, UnpicklingError, UnicodeDecodeError) as _:
                    logger.error("Cannot read file " + qualified_filename + ".")
                    raise
        else:
            logger.warn("Input file " + filename + " is not an XML data product.")
            continue

        if len(data_filenames) == 0:
            continue

        # Set up the search path for data files
        data_search_path = (os.path.split(qualified_filename)[0] + ":" +
                            os.path.split(qualified_filename)[0] + "/data:" +
                            os.path.split(qualified_filename)[0] + "/..:" +
                            os.path.split(qualified_filename)[0] + "/../data:" + search_path)

        # Search for and symlink each data file
        for data_filename in data_filenames:

            if data_filename is None or data_filename == "None" or data_filename == "data/None":
                continue

            # Find the qualified location of the data file
            try:
                qualified_data_filename = find_file(data_filename, path = data_search_path)
            except RuntimeError as _:
                # Try searching for the file without the "data/" prefix
                try:
                    qualified_data_filename = find_file(data_filename.replace("data/", "", 1), path = data_search_path)
                except RuntimeError as _:
                    raise RuntimeError("Data file " + data_filename + " cannot be found in path " + data_search_path)

            # Symlink the data file within the workdir
            if not os.path.abspath(qualified_data_filename) == os.path.abspath(
                    os.path.join(workdir.workdir, data_filename)):
                if os.path.exists(os.path.join(workdir.workdir, data_filename)):
                    os.remove(os.path.join(workdir.workdir, data_filename))
                    try:
                        os.unlink(os.path.join(workdir.workdir, data_filename))
                    except Exception as _:
                        pass
                os.symlink(qualified_data_filename, os.path.join(workdir.workdir, data_filename))

        # End loop "for data_filename in data_filenames:"

    # End loop "for input_port_name in args_to_set:"

    # Write out the new ISF
    with open(qualified_isf_filename, 'w') as fo:
        # Write out values we want set specifically
        for arg in args_to_set:
            fo.write(arg + "=" + args_to_set[arg] + "\n")

    # Symlink to *.bin files...
    binary_config_files = [fname for fname in os.listdir(args.workdir)
                           if fname.endswith('bin')]
    for bin_conf_file in binary_config_files:
        new_conf_file_name = os.path.join(workdir.workdir, bin_conf_file)
        if not os.path.exists(new_conf_file_name):
            os.symlink(os.path.join(args.workdir, bin_conf_file),
                       new_conf_file_name)

    # Inputs for thread
    simulate_inputs = inputs_tuple(*[
        args_to_set['simulation_config'],
        args_to_set['ksb_training_data'],
        args_to_set['lensmc_training_data'],
        args_to_set['momentsml_training_data'],
        args_to_set['regauss_training_data'],
        args_to_set['pipeline_config'],
        args_to_set['mdb'],
        args_to_set["bins_description"]])

    return simulate_inputs

    


def she_simulate_and_measure_bias_statistics(simulation_config,
                                             ksb_training_data,
                                             lensmc_training_data, momentsml_training_data,
                                             regauss_training_data, pipeline_config, mdb, 
                                             bins_description, workdirTuple,
                                             simulation_no, logdir, est_shear_only):
    """ Parallel processing parts of bias_measurement pipeline

    """
    # several commands...
    # @FIXME: check None types.

    workdir = workdirTuple.workdir

    data_image_list = os.path.join('data', 'data_images.json')
    stacked_data_image = os.path.join('data', 'stacked_image.xml')
    psf_images_and_tables = os.path.join('data', 'psf_images_and_tables.json')
    segmentation_images = os.path.join('data', 'segmentation_images.json')
    stacked_segmentation_image = os.path.join('data', 'stacked_segm_image.xml')
    detections_tables = os.path.join('data', 'detections_tables.json')
    details_table = os.path.join('data', 'details_table.xml')

    she_simulate_images(simulation_config, pipeline_config, data_image_list,
                        stacked_data_image, psf_images_and_tables, segmentation_images,
                        stacked_segmentation_image, detections_tables, details_table,
                        workdir, logdir, simulation_number)

    shear_estimates_product = os.path.join('data', 'shear_estimates_product.xml')
    she_lensmc_chains = os.path.join('data', 'she_lensmc_chains.xml')

    she_estimate_shear(data_images = data_image_list,
                       stacked_image = stacked_data_image,
                       psf_images_and_tables = psf_images_and_tables,
                       segmentation_images = segmentation_images,
                       stacked_segmentation_image = stacked_segmentation_image,
                       detections_tables = detections_tables,
                       ksb_training_data = ksb_training_data,
                       lensmc_training_data = lensmc_training_data,
                       momentsml_training_data = momentsml_training_data,
                       regauss_training_data = regauss_training_data,
                       pipeline_config = pipeline_config,
                       mdb = mdb,
                       shear_estimates_product = shear_estimates_product,
                       she_lensmc_chains = she_lensmc_chains,
                       workdir = workdir, logdir = logdir, sim_number = simulation_number)

    # Complete after shear only if option set.
    if est_shear_only:
        logger.info("Configuration set up to complete after shear measurement")
        return

    she_bias_statistics = os.path.join('data', 'she_bias_statistics.xml')

    she_measure_statistics(details_table=details_table,
                           shear_estimates=shear_estimates_product,
                           pipeline_config=pipeline_config,
                           she_bias_statistics=she_bias_statistics,
                           bins_description=bins_description,
                           workdir=workdir, logdir=logdir, sim_no=simulation_no)

    she_bias_measurements = os.path.join('data',
                                         'shear_bias_measurements_sim%s.xml' % simulation_number)

    # ii=0
    # maxNTries=5
    # hasRun = False
    # while not hasRun and ii<maxNTries:
    #    if os.path.exists(she_bias_statistics):

    she_cleanup_bias_measurement(simulation_config = simulation_config,
                                 data_images = data_image_list, stacked_data_image = stacked_data_image,
                                 psf_images_and_tables = psf_images_and_tables,
                                 segmentation_images = segmentation_images,
                                 stacked_segmentation_image = stacked_segmentation_image,
                                 detections_tables = detections_tables,
                                 details_table = details_table,
                                 shear_estimates = shear_estimates_product,
                                 shear_bias_statistics_in = she_bias_statistics,
                                 pipeline_config = pipeline_config,
                                 she_bias_measurements = she_bias_measurements,
                                 workdir = workdir, logdir = logdir, sim_number = simulation_number)

    logger.info("Completed parallel pipeline stage, she_simulate_and_measure_bias_statistics")


def simulate_and_measure_mapped(args):
    return she_simulate_and_measure_bias_statistics(*args)


def run_pipeline_from_args(args):
    """Main executable to run parallel pipeline.
    """

    # Check the arguments
    chosen_pipeline_info = check_args(args)  # add argument there..
    _sim_plan_table, sim_plan_tablename = rp.create_plan(args, return_table = True)

    # Create the pipeline_config for this run
    config_filename = rp.create_config(args, config_keys = chosen_pipeline_info.config_keys)
    # Create the ISF for this run

    shear_bias_measurement_listfile = os.path.join(
        args.workdir, 'data', 'shear_bias_measurement_list.json')
    if os.path.exists(shear_bias_measurement_listfile):
        os.remove(shear_bias_measurement_listfile)

    # prepare configuration

    simulation_configs = os.path.join('data', 'sim_configs.json')

    # @FIXME: sim configuration template
    base_isf = find_file(args.isf, path = args.workdir)
    # read get
    args_to_set = {}
    with open(base_isf, 'r') as fi:
        # Check each line to see if values we'll overwrite are specified in it,
        # and only write out lines with other values
        for line in fi:
            split_line = line.strip().split('=')
            # Add any new args here to the list of args we want to set
            if not (split_line[0] in args_to_set) and len(split_line) > 1:
                args_to_set[split_line[0]] = split_line[1]
    # Overwrite with any values in isf_args
    for i in range(len(args.isf_args) // 2):
        key = args.isf_args[2 * i]
        val = args.isf_args[2 * i + 1]
        if not key in args_to_set:
            raise ValueError("Unrecognized isf arg: " + str(key))
        args_to_set[key] = val

    if not ('config_template' in args_to_set and
            os.path.exists(find_file(args_to_set['config_template']))):
        raise FileExistsError("configuration template not found")

    config_template = find_file(args_to_set['config_template'])

    logger.info("Preparing configurations")
    she_prepare_configs(sim_plan_tablename,
                        config_template, simulation_configs, args.workdir)

    batches, workdir_list = create_batches(args, simulation_configs)

    logger.info("Running parallel part of pipeline in %s batches and %s threads"
                % (len(batches), args.number_threads))

    pool = multiprocessing.Pool(processes = args.number_threads)

    simulate_and_measure_args_list = []

    for batch_number in range(len(batches)):
        batch = batches[batch_number]
        # Move data to threads
        # insert_data_to_threads(args,batch,workdirList,sim_table)
        # Update_isf_...

        # Create the pipeline_config for this run
        # @TODO: Do we need multiple versions of this, one for each thread?

        for thread_number in range(batch.nThreads):
            workdir = workdir_list[thread_number + args.number_threads * batch_number]
            # logdir?
            simulation_number = get_sim_number(thread_number, batch)
            # Create the ISF for this run
            # @TODO:  do we need multiple versions of this, one for each thread
            # @FIXME: Don't really need ISF - that is for the pipeline runner..
            simulate_measure_inputs = create_simulate_measure_inputs(args,
                                                                     config_filename, workdir, simulation_configs,
                                                                     simulation_number)

            simulate_and_measure_args_list.append((simulate_measure_inputs.simulation_config,
                                                   simulate_measure_inputs.ksb_training_data,
                                                   simulate_measure_inputs.lensmc_training_data,
                                                   simulate_measure_inputs.momentsml_training_data,
                                                   simulate_measure_inputs.regauss_training_data,
                                                   simulate_measure_inputs.pipeline_config,
                                                   simulate_measure_inputs.mdb,
                                                   simulate_measure_inputs.bins_description,
                                                   workdir, simulation_no, args.logdir, args.est_shear_only))

    if simulate_and_measure_args_list:
        pool.map(simulate_and_measure_mapped, simulate_and_measure_args_list)

    if args.est_shear_only:
        logger.info("Configuration set up to complete after shear estimated: will not merge shear measurement files.")
    else:
        # Clean up
        logger.info("Cleaning up batch files..")
        for batch_number in range(len(batches)):
            merge_outputs(workdir_list, batches[batch_number], shear_bias_measurement_listfile,
                          parent_workdir = args.workdir)
            pu.cleanup(batches[batch_number], workdir_list)

    if args.est_shear_only:
        logger.info("Pipeline completed!")
        return

    # Run final process
    shear_bias_measurement_final = os.path.join(args.workdir, 'shear_bias_measurements_final.xml')
    
    #symlink the bins description from the ISF into measure_bias's workdir so it can be used
    qualified_bins = find_file(args_to_set["bins_description"])
    print(qualified_bins, type(qualified_bins))
    bins_desc = "data/bins.xml"
    print(os.path.join(args.workdir,bins_desc), type(os.path.join(args.workdir,bins_desc)))
    os.symlink(qualified_bins,os.path.join(args.workdir,bins_desc))

    logger.info("Running final she_measure_bias to calculate "
                "final shear: output in %s" % shear_bias_measurement_final)
    she_measure_bias(shear_bias_measurement_listfile, config_filename,
                     shear_bias_measurement_final, bins_desc, args.workdir, args.logdir)
    logger.info("Pipeline completed!")


def merge_outputs(workdir_list, batch,
                  shear_bias_measurement_listfile, parent_workdir):
    """ Merge outputs from different threads at the end of each
    batch. Updates .json file


    """

    new_list = []
    for workdir in workdir_list:
        thread_number = int(workdir.workdir.split('thread')[-1].split('_')[0])
        if thread_number < batch.nThreads:
            sim_number = get_sim_number(thread_number, batch)
            # @TODO: root of this in one place
            shear_bias_measurements_file = os.path.join('data', 'shear_bias_measurements_sim%s.xml' % sim_number)
            qualified_shear_bias_measurements_file = os.path.join(workdir.workdir, shear_bias_measurements_file)
            if os.path.exists(qualified_shear_bias_measurements_file):
                new_list.append(qualified_shear_bias_measurements_file)

                # Get all data files this product points to and symlink them to the main data dir
                p = read_xml_product(shear_bias_measurements_file, workdir = workdir.workdir)

                data_files = p.get_all_filenames()

                for data_file in data_files:

                    if data_file is None or data_file == "None" or data_file == "data/None" or data_file == "" or \
                            data_file == "data/":
                        continue

                    old_qualified_data_file_filename = os.path.join(workdir.workdir, data_file)
                    new_qualified_data_file_filename = os.path.join(parent_workdir, data_file)

                    if not os.path.exists(old_qualified_data_file_filename):
                        logger.warn("Expected file " + old_qualified_data_file_filename + " does not exist")

                    new_subpath = os.path.split(new_qualified_data_file_filename)[0]
                    if not os.path.exists(new_subpath):
                        os.makedirs(new_subpath)
                    os.symlink(old_qualified_data_file_filename, new_qualified_data_file_filename)

    sbml_list = []
    if os.path.exists(shear_bias_measurement_listfile):
        sbml_list = read_listfile(shear_bias_measurement_listfile)
    sbml_list.extend(new_list)
    write_listfile(shear_bias_measurement_listfile, sbml_list)

    # What are the main outputs needed for 2nd part?
    # rename? she_bias_measurements,

    # All the she_bias_measurements -- collate into .json file
