Installation
============

All Euclid projects will be deployed via cvmfs. If this is installed and
set up, this project will be pre-installed and no further work will be
necessary. In case cvmfs isn't installed, or you wish to install an
unreleased build or older build, you can do so through the following
process:

.. code:: bash

    cd ${HOME}/Work/Projects
    git clone https://gitlab.euclid-sgs.uk/PF-SHE/SHE_Pipeline.git
    cd SHE_Pipeline
    git checkout <desired branch or tag>
    make
    make test
    make install
