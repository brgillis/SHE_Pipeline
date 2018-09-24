""" @file pipeline_utilities.py

    Created Aug 2018

    Utility functions for the parallel pipeline
"""

__updated__ = "2018-09-18"

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
import argparse
from subprocess import Popen, PIPE, STDOUT
import time

from SHE_PPT.logging import getLogger
from SHE_PPT.utility import get_arguments_string


def get_relpath(file_path,workdir):
    """Removes workdir from path if necessary 
    @todo: should be in file_io?
    
    
    """
    # If workdir doesn't exist, this will not work
    if not os.path.exists(workdir):
        raise Exception("Work directory %s does not exist" 
                        % workdir)
    
    # Don't check to see if the file_path exists: it might
    # be an output.
    
    if not file_path.startswith(workdir):
        return file_path
    else:
        return os.path.relpath(file_path,workdir)
        

def create_thread_dir_struct(args,workdir_root_list,number_threads,number_batches):
    """ Used in check_args to create thread directories based on number
    threads
    
    Takes basic workdir base(s) and creates directory structure based 
    on threads from there, with data, cache and logdirs.
    
    @return: List of directories
    @rtype:  list(namedtuple)
    """
    logger = getLogger(__name__)

    # Creates directory structure
    dir_struct_tuple = namedtuple("Directories","workdir logdir app_workdir app_logdir")
    # @FIXME: Do the create multiple threads here
    for workdir_base in workdir_root_list:

        # Does the workdir exist?
        if not os.path.exists(workdir_base):
            # Can we create it?
            try:
                os.mkdir(workdir_base)
            except Exception as e:
                logger.error("Workdir base (" + workdir_base + ") does not exist and cannot be created.")
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
                logger.error("Cache directory (" + cache_dir + ") does not exist and cannot be created.")
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
                logger.error("Data directory (" + data_dir + ") does not exist and cannot be created.")
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
                logger.error("Log directory (" + log_dir + ") does not exist and cannot be created.")
                raise e
        if args.cluster:
            os.chmod(log_dir, 0o777)    
    
    # Now make multiple threads below...
        
    direct_str_list=[]        
    for thread_no in range(number_threads):
        for batch_no in range(number_batches):
            thread_dir_list=[]
            for workdir_base in workdir_root_list:
                workdir=os.path.join(workdir_base,'thread%s_batch%s' % (thread_no,batch_no)) 
                if not os.path.exists(workdir):
                    try:
                       os.mkdir(workdir)
                    except Exception as e:
                        logger.error("Workdir (" + workdir + ") does not exist and cannot be created.")
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
                        logger.error("Cache directory (" + cache_dir + ") does not exist and cannot be created.")
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
                        logger.error("Data directory (" + data_dir + ") does not exist and cannot be created.")
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
                        logger.error("logdir (" + qualified_logdir + ") does not exist and cannot be created.")
                        raise e
                if args.cluster:
                    os.chmod(qualified_logdir, 0o777)
                thread_dir_list.extend((workdir,qualified_logdir))
            if len(workdir_root_list)==1:
                thread_dir_list.extend((None,None))
            direct_str_list.append(dir_struct_tuple(*thread_dir_list))
    return direct_str_list

def cleanup(batch,workdir_list):
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

                    

       
def external_process_run(command, stdIn='', raiseOnError=True, parseStdOut=True, cwd=None,
        env=None, close_fds=True, isVerbose=True, _isIterable=False,
        ignoreMsgs=None):
    """
    Run the given external program. Unless overridden, an exception is thrown
    on error and the output is logged. Originally written by Ross Collins for VDFS


    @param command:      Command string to execute. Use a single string with
                         all arguments to run in shell, use a list of the
                         command with arguments as separate elements to not
                         run in a shell (faster if shell not needed).
    @type  command:      str or list(str)
    @param stdIn:        Optionally supply some input for stdin.
    @type  stdIn:        str
    @param raiseOnError: If True, if the external process sends anything to
                         stdErr then an exception is raised and the complete
                         programme is logged. Otherwise, stdErr is just always
                         redirected to stdOut.
    @type  raiseOnError: bool
    @param parseStdOut:  If True, stdout is captured, not print to screen, and
                         returned by this function, otherwise stdout is left
                         alone and will be sent to terminal as normal.
    @type  parseStdOut:  bool
    @param cwd:          Run the external process with this directory as its
                         working directory.
    @type  cwd:          str
    @param env:          Environment variables for the external process.
    @type  env:          dict(str:str)
    @param close_fds:    If True, close all open file-like objects before
                         executing external process.
    @type  close_fds:    bool
    @param isVerbose:    If False, don't log the full command that was
                         executed, even when Logger is in verbose mode.
    @type  isVerbose:    bool
    @param _isIterable:  Return an iterable stdout. NB: Use the L{out()}
                         function instead of this option.
    @type  _isIterable:  bool
    @param ignoreMsgs:   List of strings that if they appear in stderr should
                         override the raiseOnError if it is set to True.
    @type  ignoreMsgs:   list(str)

    @return: Messages sent to stdout if parsed, otherwise an iterable file
             object for stdout if _isIterable, else a return a code.
    @rtype:  list(str) or file or int


    @obsolete? Not currently used.... 
    """
    # @FIXME: What is reported as stdErr is often stdOut.
    # Why?? 
    # Am I using the wrong PROC/PIPE?
    # IF WARN / INFO etc --> stdout
    # IF ERROR / Exception --> srderr 
    
    
    
    
    logger = getLogger(__name__)
    cmdStr = (command if isinstance(command, str) else ' '.join(command))
    if isVerbose:
        logger.info(cmdStr)

    parseStdOut = parseStdOut or _isIterable
    parseStdErr = True #raiseOnError and not _isIterable
    isMemError = False
    while True:
        try:
            # @TODO: Is this the best command.
            # Why do info go to stderr.
            proc = Popen(command, shell=isinstance(command, str),
                         stdin=(PIPE if stdIn else None),
                         stdout=(PIPE if parseStdOut else None),
                         stderr=(PIPE if parseStdErr else STDOUT),
                         close_fds=close_fds, cwd=cwd, env=env)
        except OSError as error:
            if "[Errno 12] Cannot allocate memory" not in str(error):
                raise
            if not isMemError:
                logger.info("Memory allocation problem; delaying...")
                isMemError = True
                close_fds = True
            time.sleep(60)
        else:
            if isMemError:
                logger.info("Problem fixed; continuing...")
            break

    if stdIn:
        proc.stdin.write(stdIn + '\n')
        proc.stdin.flush()

    if _isIterable:
        return proc.stdout

    stdOut = []
    stdErr = []
    try:
        if parseStdOut:
            # Calling readlines() instead of iterating through stdout ensures
            # that KeyboardInterrupts are handled correctly.
            stdOut = [line.strip() for line in proc.stdout.readlines()]
            #if raiseOnError:
            #
            stdErrInit = [line.strip() for line in proc.stderr.readlines()]
            stdErr = [line for line in stdErrInit
                if 'ERROR' in str(line.upper()) or 'EXCEPTION' in str(line.upper())]
            stdOut += [line for line in stdErrInit
                if not ('ERROR' in str(line.upper()) or 
                        'EXCEPTION' in str(line.upper()))]
        if not parseStdOut and not raiseOnError:
            return proc.wait()
#    except KeyboardInterrupt:#        # Block future keyboard interrupts until process has finished cleanly
#        with utils.noInterrupt():
#            Logger.addMessage("KeyboardInterrupt - %s interrupted, "
#              "waiting for process to end cleanly..." %
#              os.path.basename(command.split()[0]))
#            if parseStdOut:
#                print(''.join(proc.stdout))
#            if parseStdErr:
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
    if stdErr and ignoreMsgs:
        for stdErrStr in stdErr[:]:
            if any(msg in stdErrStr for msg in ignoreMsgs):
                stdErr.remove(stdErrStr)

    if stdErr:
        if raiseOnError and (not isVerbose):
            logger.error(cmdStr)

        for line in stdOut:
            logger.error(line)

        for line in stdErr:
            logger.error('# ' + str(line))

        if raiseOnError:
            cmd = cmdStr.split(';')[-1].split()[0]
            if cmd == "python":
                cmd = ' '.join(cmdStr.split()[:2])

            raise Exception(cmd + " failed", stdErr)

    return stdOut,stdErr 

def create_logs(log_directory,fileName,std_out,std_err):
    """ @fixme: logging not properly working
    remove this when I get it working
    
    @obsolete? no longer used.
    """
    stdout_filename=os.path.join(log_directory,fileName+".log")
    stderr_filename=os.path.join(log_directory,fileName+".err")
    
    stdout_lines=[str(line) for line in std_out]
    open(stdout_filename,'w').writelines(stdout_lines)
    
    stdout_lines=[str(line) for line in std_err]
    open(stderr_filename,'w').writelines(stdout_lines)
    
    return

def setup_function_args(argv, command_line_int_ref,execName):
    """
    """
    logger=getLogger(__name__)
    
    estshr_args_parser= command_line_int_ref.defineSpecificProgramOptions()
    
    # add arg --log-file
    estshr_args_parser.add_argument('--log-file',type=str,
        help='XML data product to contain file links to the shear estimates tables.')
    
        
    estshr_args= estshr_args_parser.parse_args(argv)
    exec_cmd = get_arguments_string(estshr_args, cmd=execName,
                                    store_true=["profile", "debug", "dry_run"])
    logger.info('Execution command for this step:')
    logger.info(exec_cmd)  
    
    return estshr_args