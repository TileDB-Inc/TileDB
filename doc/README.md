# Building the ReadTheDocs docs locally.

Creates a Python virtual env 'venv' in the current directory, and runs the sphix build process.
    
    $ ./local-build.sh # (current dir)

## Conda Env:

To create an isolated conda environment for installing / running sphinx:
    $ conda env create -f source/conda-env.yaml
    $ source activate tiledb-env
    $ ./local build.sh

**Note** If you get errors running the script, try removing the local `venv` folder and re-running the script.
