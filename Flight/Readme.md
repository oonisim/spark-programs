# Flight data analysis

## Assumptions
* Data is cleaned and not erroneous, e.g. no duplicate, no NaN, no null, incorrect format, etc.
* No need to consider time zone
* "More than N" means N is not included. e.g. More than 3 is 4, 5, 6, ...
* Duplicating same information is to be removed. e.g. (Smith, John, 15) and (John, Smith 15) meaning Smith and John share 15 flights are to be reduced to (John, Smith, 15) only.
* Sorting is per task title,e.g if "longest run", the sort by "longest run" column.

## Data
### flightData.csv

|     Field    | Description | 
| -------------|:-------------| 
| passengerId | Integer representing the id of a passenger | 
| flightId |Integer representing the id of a flight | 
| from | String representing the departure country | 
| to   | String representing the destination country | 
| date | String representing the date of a flight | 

## Spark via Jupyter Notebook

Using [Apache Toree](https://toree.apache.org/), which is the tool to use Apache Spark via Jupyter Notebook.

### Anaconda installation

```
# https://docs.anaconda.com/anaconda/install/silent-mode/#
CONDA_INSTALLER='Anaconda3-2019.03-Linux-x86_64.sh'
curl -O https://repo.anaconda.com/archive/${CONDA_INSTALLER}
/bin/bash ${CONDA_INSTALLER} -b -f -p $HOME/conda
 
echo -e '\nexport PATH=$HOME/conda/bin:$PATH' >> $HOME/.bashrc && source $HOME/.bashrc
conda config --set auto_activate_base true
conda init
``` 

### Create and activate Anaconda environment
```aidl
EXPORT ENV='YOUR ENVIRONMENT/PROJECT NAME'
EXPORT SPARK_HOME='YOUR SPARK HOME'

conda create -n ${ENV} python=3.6
conda activate ${ENV}
 
pip install --upgrade toree
jupyter toree install --spark_home=${SPARK_HOME} --interpreters=Scala,PySpark,SQL --user
#SPARK_OPTS='--master=local[*]' jupyter notebook
SPARK_OPTS='--master spark://${MASTER}:7077 --deploy-mode client --num-executors 2 --driver-memory 4g --executor-memory 5g --executor-cores 4' jupyter notebook
```

# Output Directory Structure

```
Flight/src/main/
├── jupyter
│   ├── 01_flights_per_month.ipynb       <--- Q1 jupyter notebook
│   ├── 02_frequent_flyers.ipynb         <--- Q2 jupyter notebook
│   ├── 03_longest_run.ipynb             <--- Q3 jupyter notebook
│   ├── 04_flights_together.ipynb        <--- Q4 SQL version jupyter notebook
│   ├── 04.flights_together_matrix.ipynb <--- Q4 Matrix version jupyter notebook
│   ├── 04.flights_together_bonus.ipynb  <--- Q4 bonus one jupyter notebook
│   ├── misc.ipynb
│   ├── results
│   │   ├── flightsPerMonth.csv          <--- Q1 : Sorted by month
│   │   ├── topFrequentFlyers.csv        <--- Q2 : Sorted by number of flights
│   │   ├── longestRun.csv               <--- Q3 : Sorted by longest run
│   │   ├── flightsTogether.csv          <--- Q4 : Sorted by number of shared flights (num > 3) 
│   │   └── flightsTogetherMatrix.csv 
│   └── setup.sh
└── resources
    ├── flightData.csv
    ├── passengers.csv
    └── test.csv                         <--- For Q4 matrix version simple test
```


# Runtime environment

|   Component    | Description | 
| -------------|:-------------| 
| OS | 18.04.3 LTS (Bionic Beaver) | 
| JDK | build 1.8.0_222-8u222-b10-1ubuntu1~18.04.1-b10 | 
| Scala | 2.11.12 |
| Spark | 2.4.4 | 
| Python | Python 3.6.9 :: Anaconda, Inc|
| Toree | 0.3.0 |

