""" @file pipeline_utilities.py

    Created Aug 2018

    Utility functions for the parallel pipeline
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

from collections import namedtuple
import os
from subprocess import Popen, PIPE, STDOUT
import time

from EL_PythonUtils.utilities import get_arguments_string
from SHE_PPT.logging import getLogger

# Creates directory structure
dir_struct_tuple = namedtuple("dir_struct_tuple", "workdir logdir app_workdir app_logdir")


def get_relpath(file_path, workdir):
    """Removes workdir from path if necessary
    @todo: should be in file_io?


    """
    # If workdir doesn't exist, this will not work
    if not os.path.exists(workdir):
        raise FileNotFoundError("Work directory %s does not exist" % workdir)

    # Don't check to see if the file_path exists: it might
    # be an output.

    if not file_path.startswith(workdir):
        return file_path
    else:
        return os.path.relpath(file_path, workdir)


def create_thread_dir_struct(args, workdir_root_list, number_threads, number_batches):
    """ Used in check_args to create thread directories based on number
    threads

    Takes basic workdir base(s) and creates directory structure based
    on threads from there, with data, cache and logdirs.

    @return: List of directories
    @rtype:  list(namedtuple)
    """
    logger = getLogger(__name__)
    # @FIXME: Do the create multiple threads here
    for workdir_base in workdir_root_list:

        # Does the workdir exist?
        if not os.path.exists(workdir_base):
            # Can we create it?
            try:
                os.mkdir(workdir_base)
            except Exception as e:
                logger.error(f"Workdir base ({workdir_base}) does not exist and cannot be created.")
                raise e
        if args.cluster:
            os.chmod(workdir_base, 0o777)
        # Does the cache directory exist within the workdir?
        cache_dir = os.path.join(workdir_base, "cache")
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
        data_dir = os.path.join(workdir_base, "data")
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
        log_dir = os.path.join(workdir_base, args.logdir)
        if not os.path.exists(log_dir):
            # Can we create it?
            try:
                os.mkdir(log_dir)
            except Exception as e:
                logger.error(f"Log directory ({log_dir}) does not exist and cannot be created.")
                raise e
        if args.cluster:
            os.chmod(log_dir, 0o777)

    # Now make multiple threads below...

    direct_str_list = []
    for batch_no in range(number_batches):
        for thread_no in range(number_threads):
            thread_dir_list = []
            for workdir_base in workdir_root_list:
                workdir = os.path.join(workdir_base, 'thread%s_batch%s' % (thread_no, batch_no))
                if not os.path.exists(workdir):
                    try:
                        os.mkdir(workdir)
                    except Exception as e:
                        logger.error(f"Workdir ({workdir}) does not exist and cannot be created.")
                        raise e
                if args.cluster:
                    os.chmod(workdir, 0o777)

                # Does the cache directory exist within the workdir?
                cache_dir = os.path.join(workdir, "cache")
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
                data_dir = os.path.join(workdir, "data")
                if not os.path.exists(data_dir):
                    # Can we create it?
                    try:
                        os.mkdir(data_dir)
                    except Exception as e:
                        logger.error(f"Data directory ({data_dir}) does not exist and cannot be created.")
                        raise e
                if args.cluster:
                    os.chmod(data_dir, 0o777)

                # Does the logdir exist?
                qualified_logdir = os.path.join(workdir, args.logdir)
                if not os.path.exists(qualified_logdir):
                    # Can we create it?
                    try:
                        os.mkdir(qualified_logdir)
                    except Exception as e:
                        logger.error(f"logdir ({qualified_logdir}) does not exist and cannot be created.")
                        raise e
                if args.cluster:
                    os.chmod(qualified_logdir, 0o777)
                thread_dir_list.extend((workdir, qualified_logdir))
            if len(workdir_root_list) == 1:
                thread_dir_list.extend((None, None))
            direct_str_list.append(dir_struct_tuple(*thread_dir_list))
    return direct_str_list


def cleanup(batch, workdir_list):
    """
    Remove sim links and batch setup files ready for the next batch.
    Remove intermediate products...


    """
    # workdir:
    # Each thread - not sim* files... (or there components...)

    pass


def run_threads(threads):
    """ Executes given list of thread processes.
    Originally written by Ross Collins for VDFS
    Modified for circumstances...

    """

    logger = getLogger(__name__)
    try:
        for thread in threads:
            thread.start()
    finally:
        for thread in threads:

            thread.join()

            if thread.exitcode:
                logger.info("<ERROR> Thread failed. Terminating thread")


def external_process_run(command, std_in='', raise_on_error=True, parse_std_out=True, cwd=None,
                         env=None, close_fds=True, is_verbose=True, _is_iterable=False,
                         ignore_msgs=None):
    """
    Run the given external program. Unless overridden, an exception is thrown
    on error and the output is logged. Originally written by Ross Collins for VDFS


    @param command:      Command string to execute. Use a single string with
                         all arguments to run in shell, use a list of the
                         command with arguments as separate elements to not
                         run in a shell (faster if shell not needed).
    @type  command:      str or list(str)
    @param std_in:        Optionally supply some input for stdin.
    @type  std_in:        str
    @param raise_on_error: If True, if the external process sends anything to
                         std_err then an exception is raised and the complete
                         programme is logged. Otherwise, std_err is just always
                         redirected to std_out.
    @type  raise_on_error: bool
    @param parse_std_out:  If True, stdout is captured, not print to screen, and
                         returned by this function, otherwise stdout is left
                         alone and will be sent to terminal as normal.
    @type  parse_std_out:  bool
    @param cwd:          Run the external process with this directory as its
                         working directory.
    @type  cwd:          str
    @param env:          Environment variables for the external process.
    @type  env:          dict(str:str)
    @param close_fds:    If True, close all open file-like objects before
                         executing external process.
    @type  close_fds:    bool
    @param is_verbose:    If False, don't log the full command that was
                         executed, even when Logger is in verbose mode.
    @type  is_verbose:    bool
    @param _is_iterable:  Return an iterable stdout. NB: Use the L{out()}
                         function instead of this option.
    @type  _is_iterable:  bool
    @param ignore_msgs:   List of strings that if they appear in stderr should
                         override the raise_on_error if it is set to True.
    @type  ignore_msgs:   list(str)

    @return: Messages sent to stdout if parsed, otherwise an iterable file
             object for stdout if _is_iterable, else a return a code.
    @rtype:  list(str) or file or int


    @obsolete? Not currently used....
    """
    # @FIXME: What is reported as std_err is often std_out.
    # Why??
    # Am I using the wrong PROC/PIPE?
    # IF WARN / INFO etc --> stdout
    # IF ERROR / Exception --> srderr

    logger = getLogger(__name__)
    cmd_str = (command if isinstance(command, str) else ' '.join(command))
    if is_verbose:
        logger.info(cmd_str)

    parse_std_out = parse_std_out or _is_iterable
    parse_std_err = True  # raise_on_error and not _is_iterable
    is_mem_error = False
    while True:
        try:
            # @TODO: Is this the best command.
            # Why do info go to stderr.
            proc = Popen(command, shell=isinstance(command, str),
                         stdin=(PIPE if std_in else None),
                         stdout=(PIPE if parse_std_out else None),
                         stderr=(PIPE if parse_std_err else STDOUT),
                         close_fds=close_fds, cwd=cwd, env=env)
        except OSError as error:
            if "[Errno 12] Cannot allocate memory" not in str(error):
                raise
            if not is_mem_error:
                logger.info("Memory allocation problem; delaying...")
                is_mem_error = True
                close_fds = True
            time.sleep(60)
        else:
            if is_mem_error:
                logger.info("Problem fixed; continuing...")
            break

    if std_in:
        proc.stdin.write(std_in + '\n')
        proc.stdin.flush()

    if _is_iterable:
        return proc.stdout

    std_out = []
    std_err = []
    try:
        if parse_std_out:
            # Calling readlines() instead of iterating through stdout ensures
            # that KeyboardInterrupts are handled correctly.
            std_out = [line.strip() for line in proc.stdout.readlines()]
            # if raise_on_error:
            #
            std_err_init = [line.strip() for line in proc.stderr.readlines()]
            std_err = [line for line in std_err_init
                       if 'ERROR' in str(line.upper()) or 'EXCEPTION' in str(line.upper())]
            std_out += [line for line in std_err_init
                        if not ('ERROR' in str(line.upper()) or
                                'EXCEPTION' in str(line.upper()))]
        if not parse_std_out and not raise_on_error:
            return proc.wait()
    #    except KeyboardInterrupt:#        # Block future keyboard interrupts until process has finished cleanly
    #        with utils.noInterrupt():
    #            Logger.addMessage("KeyboardInterrupt - %s interrupted, "
    #              "waiting for process to end cleanly..." %
    #              os.path.basename(command.split()[0]))
    #            if parse_std_out:
    #                print(''.join(proc.stdout))
    #            if parse_std_err:
    #                print(''.join(proc.stderr))
    #            proc.wait()
    #        raise
    except IOError as error:
        # Sometimes a KeyboardInterrupt is translated into an IOError - I think
        # this may just be due to a bug in PyFITS messing with signals, as only
        # seems to happen when the PyFITS ignoring KeyboardInterrupt occurs.
        if "Interrupted system call" in str(error):
            raise KeyboardInterrupt
        raise

    # If the stdErr messages are benign then ignore them
    if std_err and ignore_msgs:
        for std_err_str in std_err[:]:
            if any(msg in std_err_str for msg in ignore_msgs):
                std_err.remove(std_err_str)

    if std_err:
        if raise_on_error and (not is_verbose):
            logger.error(cmd_str)

        for line in std_out:
            logger.error(line)

        for line in std_err:
            logger.error('# ' + str(line))

        if raise_on_error:
            cmd = cmd_str.split(';')[-1].split()[0]
            if cmd == "python":
                cmd = ' '.join(cmd_str.split()[:2])

            raise Exception(cmd + " failed", std_err)

    return std_out, std_err


def create_logs(log_directory, file_name, std_out, std_err):
    """ @fixme: logging not properly working
    remove this when I get it working

    @obsolete? no longer used.
    """
    stdout_filename = os.path.join(log_directory, file_name + ".log")
    stderr_filename = os.path.join(log_directory, file_name + ".err")

    stdout_lines = [str(line) for line in std_out]
    open(stdout_filename, 'w').writelines(stdout_lines)

    stdout_lines = [str(line) for line in std_err]
    open(stderr_filename, 'w').writelines(stdout_lines)


def setup_function_args(argv, command_line_int_ref, exec_name):
    """
    """
    logger = getLogger(__name__)

    estshr_args_parser = command_line_int_ref.defineSpecificProgramOptions()

    # add arg --log-file
    estshr_args_parser.add_argument('--log-file', type=str,
                                    help='XML data product to contain file links to the shear estimates tables.')

    estshr_args = estshr_args_parser.parse_args(argv)
    exec_cmd = get_arguments_string(estshr_args, cmd=exec_name,
                                    store_true=["profile", "debug", "dry_run", "webdav_archive",
                                                "store_measurements_only", "use_bias_only"])
    logger.info('Execution command for this step:')
    logger.info(exec_cmd)

    return estshr_args
