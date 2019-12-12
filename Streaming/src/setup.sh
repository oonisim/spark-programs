echo "Source this file, not execute"

set -eu
export ENV=Dataframe

conda create -n ${ENV} python=3.6
conda activate ${ENV}

conda install jupyter notebook
pip3 install --upgrade toree
jupyter toree install --spark_home=${SPARK_HOME} --interpreters=Scala,PySpark,SQL --user

#------------------------------------------------------------------------------------------
# Run Toree
#------------------------------------------------------------------------------------------
#SPARK_OPTS='--master=local[*]' jupyter notebook

# OOM error
#SPARK_OPTS='--master spark://oonisim:7077 --deploy-mode client --num-executors 4 --driver-memory 2g --executor-memory 2g --executor-cores 4' jupyter notebook
SPARK_OPTS='--master yarn --deploy-mode client --num-executors 2 --driver-memory 2g --executor-memory 4g --executor-cores 4' jupyter notebook

