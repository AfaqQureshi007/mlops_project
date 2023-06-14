import ccxt
import pandas as pd
import datetime as dt
from prophet import Prophet
import os
import shutil
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import mlflow.prophet
import mlflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 11),
}

dag = DAG(
    'model_training_and_saving',
    default_args=default_args,
    description='A DAG to train and save a model using DVC and Google Drive',
    schedule_interval=None,
    catchup=False,
)


def make_data_and_train():
    exchange = ccxt.kraken()
    symbol = 'BTC/USD'
    timeframe = '1m'  # Use 1m instead of 1s
    limit = 700
    historical_data = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    print(historical_data)
    df = pd.DataFrame(historical_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df.drop(columns=['open', 'high', 'low', 'volume'], inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    df = df.resample('1min').agg({'close': 'last'}).dropna()  # Use 1m resampling
    df.rename(columns={'timestamp': 'ds', 'close': 'y'},inplace=True)


    os.chdir("mlops_project")

    print(os.getcwd())
    df.to_csv("data.csv",date_format='%Y-%m-%d %H:%M:%S')




def create_experiment(experiment_name):
    mlflow.create_experiment(experiment_name)

def set_tracking_uri(tracking_uri):
    mlflow.set_tracking_uri(tracking_uri)


def get_experiment_id(experiment_name):
    experiments = mlflow.tracking.MlflowClient().search_runs(experiment_ids=[], filter_string=f"tags.mlflow.runName='{experiment_name}'", run_view_type=mlflow.entities.ViewType.ALL)
    if experiments:
        return experiments[0].info.experiment_id
    return None
    
def train_model():
    try:
        create_experiment('Mlops')
    except:
        get_experiment_id('Mlops')
    set_tracking_uri('http://localhost:5000')

    with mlflow.start_run():
        mlflow.set_experiment('Mlops')


    hyperparameters = {
    'seasonality_mode': 'multiplicative',
    'changepoint_prior_scale': 0.1
    }
    model = Prophet(**hyperparameters)
    df_prophet=pd.read_csv('/home/ubuntu/mlops_project/data.csv')
    df_prophet.rename(columns={'timestamp': 'ds'},inplace=True)
    model.fit(df_prophet)






    mlflow.start_run()

    # Log the hyperparameters
    mlflow.log_params(hyperparameters)

    # Log the model
    mlflow.prophet.log_model(model, "saved_model")

    # End the MLflow run
    mlflow.end_run()



def dvc():


    os.chdir("mlops_project")

    result = subprocess.run(["dvc","init","--force"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    print(result.stdout)


    result = subprocess.run(["dvc","add","data.csv"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )



    result = subprocess.run(["dvc","remote","add","-d","storage","gdrive://1foWdizD_GzZQXzRXhrjghMLnIa_Hnh-5?usp=drive_link -f"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )


    print(result.stdout)

    result = subprocess.run(["git","add",".dvc","data.csv.dvc",".gitignore",".dvcignore"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    print(result.stdout)

    result = subprocess.run(["git","commit","-m","CommitingDVC"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    print(result.stdout)

    result = subprocess.run(["git","push","https://AfaqQureshi007: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@github.com/AfaqQureshi007/mlops_project"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    print(result.stdout)


    result = subprocess.run(["dvc","push"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True)




def clone ():
    try:
        shutil.rmtree("/home/ubuntu/mlops_project")
        result = subprocess.run(["git", "clone","https://github.com/AfaqQureshi007/mlops_project.git"], capture_output=True, text=True)

    except:
        
        result = subprocess.run(["git","clone","https://github.com/AfaqQureshi007/mlops_project.git"], capture_output=True, text=True)



t2 = PythonOperator(
    task_id='DataDownload',
    python_callable=make_data_and_train,
    dag=dag,
)



t1=PythonOperator(
    task_id='GitClone',
    python_callable=clone,
    dag=dag,
)




t3=PythonOperator(
    task_id='Train_model',
    python_callable=train_model,
    dag=dag,
)



t4=PythonOperator(
    task_id='DVC',
    python_callable=dvc,
    dag=dag,
)






t1 >> t2 >> t3 >> t4