Troubleshooting
===============

    ``If any problems are anticipated, add a section here for them to help users resolve them on their own before they need to appeal to a developer for help.``

The cat in the generated picture appears to be wearing both a standard tie and a bowtie
---------------------------------------------------------------------------------------

    ``For known problems which can occur if the user makes a common error, explain how it can be resolved.``

This is a known bug which occurs if the user requests
``--use_tie=bowtie``. The correct argument is ``--use_tie=bow``.

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
