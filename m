#!/bin/bash

set -e
set -u

python setup.py bdist_wheel
python setup.py sdist

repo_dir="${local_pypi}/$(basename $(pwd))"
mkdir -p ${repo_dir}
cp dist/*.whl ${repo_dir}
cp dist/*.tar.gz ${repo_dir}

