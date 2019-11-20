# Flight data analysis

## Assumptions
* Data is cleaned and not erroneous.

## Data
### flightData.csv

|     Field    | Description | 
| -------------|:-------------| 
| passengerId | Integer representing the id of a passenger | 
| flightId |Integer representing the id of a flight | 
| from | String representing the departure country | 
| to   | String representing the destination country | 
| date | String representing the date of a flight | 

# Runtime environment

|   Component    | Description | 
| -------------|:-------------| 
| OS | 18.04.3 LTS (Bionic Beaver) | 
| JDK | build 1.8.0_222-8u222-b10-1ubuntu1~18.04.1-b10 | 
| Scala | 2.11.12 |
| Spark | 2.4.4 | 
| Python | Python 3.6.9 :: Anaconda, Inc|
| Toree | 0.3.0 |


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
SPARK_OPTS='--master=local[*]' jupyter notebook
```



# Directory Structure

```
src/main
├── jupyter
│   ├── 01_flights_per_month.ipynb   <--- Q1 
│   ├── 02_frequent_flyers.ipynb     <--- Q2
│   ├── 03_visited_countries.ipynb   <--- Q3
│   ├── dataframe.ipynb
│   ├── results
│   ├── setup.sh
│   ├── spark-warehouse
│   └── sql.ipynb
├── resources
    ├── flightData.csv
    └── passengers.csv
```