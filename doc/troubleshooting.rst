Troubleshooting
===============

An error occurred after submitting a run to the pipeline server, and the run was never submitted
------------------------------------------------------------------------------------------------

There are a few reasons that this might occur. First, check that you have the correct URL and port for the server. If you haven't configured it yourself, the manage of the machine that it's hosted on should be able to help you here.

Next, check that the version of the IAL pipeline runner used to submit the run is the same as the version running the pipeline server. As the pipeline servers are managed separately from this code, it is possible for the version of the servers to be updated before this project is updated to match. Within this project, the version of the pipeline runner used can be found in the file ``SHE_Pipeline/python/SHE_Pipeline/run_pipeline.py`` at line 47, which states:

``pipeline_runner_version = "2.2.7"``

If you've configured the pipeline server yourself, check that this matches. Otherwise, check with the person managing the server to get the version. If this doesn't match the version of the server, update the code locally to match and re-install. Then, please open an issue on this project's GitLab repository or e-mail the active developers to inform them that this needs to be updated.

Finally, an error might occur due to an issue with the command submitting the pipeline run to the server. Normally the command which is generated and used should always be valid, but it is possible that an unanticipated edge case might occur, or that an update to the IAL pipeline runner version will make previously-valid syntax no longer valid. In either of thse cases, an error message should make clear that something is wrong with the command. Please open an issue on this project's GitLab repository or e-mail the active developers to inform them of this issue, and include a full log of your attempted execution, starting from the command you called to trigger the executable and ending with the final error message.

In the meantime, see if the error message is clear enough about the source of the error that you can fix it yourself. Copy-and-paste the submission command and replace all the arguments with ``--help`` to see information on the expected syntax, and see if you can determine from this and the error message what the issue is. If you can figure it out, call the fixed command to submit the pipeline run.

The run was submitted to the pipeline server, but an error occurred before any tasks started
--------------------------------------------------------------------------------------------

When a run is submitted to the pipeline server, the first step it performs is parsing the Pipeline Script and Package Definition to determine internally how to run the pipeline. After this, it matches the provided input files to the input ports of the pipeline. An error at this stage of execution indicates either that there is some problem with  the Pipeline Script and/or Package Definition for the pipeline you are trying to execute, or that there is a mismatch between the provided inputs and the input ports of the pipeline. If the error message refers to input ports, the latter issue is most likely, otherwise the former is most likely.

**Issue with pipeline structure**

If you've modified either the Pipeline Script or Package Definition yourself, first attempt reverting to the deployed version of the code and testing if the error persists. If it does not, then the error was most likely introduced by your changes. You will need to consider your changes and the error messages to determine how to fix the issue.

If the error is present on a deployed version of the code, please open an issue on this project's GitLab repository or e-mail the active developers to inform them that this needs to be updated. You might be able to fix this yourself by inspecting the error message and seeing what the problem is. Commonly this occurs when a call to a task isn't updated at the same time as that task's definition, resulting in the ports no longer matching up, and can be fixed by updating the call to the task.

**Issue with input**

To test whether or not this issue has been introduced by your provided input, attempt to call the pipeline with default input files (that is, call the helper script without either the ``--isf`` or ``--isf_args`` options), rename (or copy or symlink) your input files to the default filenames (which can be found in the \<pipeline\_name\>_isf.txt file in ``SHE_Pipeline/auxdir/SHE_Pipeline``), and see if this succeeds or not. If this succeeds, then this means that one of the input ports you specified in either your ISF or to the ``--isf_args`` argument is invalid. Compare to the input ports defined for the pipeline in its pipeline script, and check for any differences, such as from typos or spelling mistakes.

If you get an error message about input ports not matching up when default input is used, then please open an issue on this project's GitLab repository or e-mail the active developers to inform them of this issue. You can attempt to rectify this in the meantime by copying the default ISF from the auxdir to your workdir, and updating it so that the ports in it match those in the Pipeline Script.


A test failed when I ran "make test"
------------------------------------

**Ensure you have the most up-to-date version of the project and all
its dependencies**

It's possible the issue you're hitting is a bug that's already been
fixed, or could be due to locally-installed versions of projects on the
develop branch no longer being compatible with a newly-deployed version
of another dependency on CODEEN. If you're running on the develop branch
and have installed locally, pull the project, call ``make purge``, and
install again, and repeat for all dependencies you've installed locally.
Try running ``make test`` again to see if it works.

**Report the failing test to the developers**

If the test still fails, please report it to the active developers
listed above, ideally by creating a GitLab issue, or else by e-mailing
them.

**Try running the desired code**

Tests can fail for many reasons, and a common reason is that the code is
updated but not the test. This could lead to the test failing even if
the code works properly. After you've reported the issue, you can try to
run the desired code before the issue with the failing test has been
fixed. There's a decent chance that the bug might only be in the test
code, and the executable code will still function.

An exception was raised, what do I do?
--------------------------------------

    ``General instructions for how to figure out what to do when an exception is raised, which can be copied for all projects.``

**Check for an issue with the input**

First, look through the exception text to see if it indicates an issue
with the input data. This will often be indicated by the final raised
exception indicating an issue with reading a file, such as a
SheFileReadError which states it cannot open a file. If this is the
case, check if the file exists and is in the format that the code
expects. If the file doesn't exist, then you've found the problem.
Either a needed input file is missing, or one of the input files points
to the incorrect filename. Determine which this is, and fix it from
there.

If the file does exist but you still see an error reading from it, then
the issue is most likely that the file is unreadable for some reason -
perhaps the download was corrupt, perhaps manual editing left it
improperly formatted, etc. Try to test if this is the case by reading it
manually. For instance, if the program can't open a ``FITS`` file, try
opening it with ``astropy``, ``ds9``, ``topcat`` etc. (whatever you're
comfortable with) to see if you can read it external to the code.

Keep in mind that the code might try multiple methods to open a file.
For instance, the pipeline\_config input file can be supplied as either
a raw text file, an ``.xml`` data product, or a ``.json`` listfile. The
program will try all these options, and if all fail, the final exception
text will only show the final type attempted. The full traceback,
however, should show all attempts. So if it appears that the program
tried to read a file as the wrong type, check through the traceback to
see if it previously tried to read it as the expected type and failed.

**Ensure you have the most up-to-date version of the project and all
its dependencies**

It's possible the issue you're hitting is a bug that's already been
fixed, or could be due to locally-installed versions of projects on the
develop branch no longer being compatible with a newly-deployed version
of another dependency on CODEEN. If you're running on the develop branch
and have installed locally, pull the project, call ``make purge``, and
install again, and repeat for all dependencies you've installed locally.
Try running again to see if this works.

**See if the exception, traceback, or log gives you any other clue to
solve the problem**

There are many reasons something might go wrong, and many have been
anticipated in the code with an exception to indicate this. The
exception text might tell you explicitly what the problem is - for
instance, maybe two options you set aren't compatible together. If it
wasn't an anticipated problem, the exception text probably won't
obviously indicate the source of the problem, but you might be able to
intuit it from the traceback. Look through the traceback at least a few
steps back to see if anything jumps out at you as a potential problem
that you can fix. Also check the logging of the program for any errors
or warnings, and consider if those might be related to your problem.

**Report the issue**

If all else fails, raise an issue with the developers on GitLab. Be sure
to include the following information:

1. Any details of input data you're using.
2. The command you called to trigger the program (or the pipeline which
   called the program)
3. The full log of the execution, from the start of the program to the
   ultimate failure. In the case of a failure during a pipeline run, you
   can attach the generated log file for this executable, which can be
   found in the ``logs`` directory within the work directory, and then
   in a subdirectory corresponding to this task.
4. Any steps you've taken to try to resolve this problem on your own.
