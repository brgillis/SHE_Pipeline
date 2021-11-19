# SHE_IAL_Pipelines

The documentation for this project is stored in the `doc` folder of this project. This documentation is compilable by
Elements, and best viewed in the compiled version. This can be accessed either by:

* Accessing the documentation for a deployed version through CODEEN. For the develop version, this is available
  at: https://codeen.euclid-ec.org/jenkins/view/SHE/job/SHE_IAL_Pipelines/job/develop/Documentation

* Downloading the repository and compiling the documentation locally. This can be done through the following commands:
  ```bash
  cd ${HOME}/Work/Projects git clone https://gitlab.euclid-sgs.uk/PF-SHE/SHE_IAL_Pipelines.git
  cd SHE_IAL_Pipelines git checkout <desired branch or tag>
  make
  ```
  This will make the documentation available in the
  directory `${HOME}/Work/Projects/SHE_IAL_Pipelines/build.x86_64-conda_cos6-gcc73-o2g/doc/sphinx/html/`. The
  page `index.html` in this directory will be the main page for the documentation, and can be opened with your browser
  of choice (e.g. `firefox`) to view the compiled documentation.

Otherwise, the documentation can be read uncompiled.
