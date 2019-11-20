#!/bin/bash
set -eu

conda create -n ${ENV} python=3.6
conda activate ${ENV}

conda install jupyter notebook
pip install --upgrade toree
jupyter toree install --spark_home=${SPARK_HOME} --interpreters=Scala,PySpark,SQL --user
SPARK_OPTS='--master=local[*]' jupyter notebook

