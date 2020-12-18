from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
import datetime
import pandas as pd
import csv
import requests
import json

default_args = {
	"owner" : "airflow",
	"start_date": datetime.datetime(2020, 12, 11),
	"depends_on_past": False,
	"email_on_failure": False,
	"email_on_retry": False,
	"email": "bayu.samudra.009@gmail.com",
	"retries": 1,
	"retry_delay": datetime.timedelta(minutes=5)
}


def download_data():
	data = []
	api = 'https://api.kawalcorona.com/indonesia/provinsi/'
	response = requests.get(api).json()
	tanggal = str(datetime.date.today())
	for row in response["features"]:
	    provinsi = row["attributes"]["Provinsi"]
	    positif = row["attributes"]["Kasus_Posi"]
	    sembuh = row["attributes"]["Kasus_Semb"]
	    meninggal = row["attributes"]["Kasus_Meni"]
	    kasus = {"tanggal": tanggal, "provinsi": provinsi, "positif": positif, "sembuh": sembuh, "meninggal": meninggal}
	    data.append(kasus)

	with open(f'/home/bayu_samudra_009/airflow/dags/files/covid_{tanggal}.json', 'w') as file:
	    json.dump(data, file)

with open('/home/bayu_samudra_009/airflow/dags/files/slack_token', mode='r') as text:
	pasw = text.readline()

token = str(pasw).strip('\n')


with DAG(dag_id="data_pipeline", schedule_interval="@daily", default_args= default_args, catchup=False) as dag:
	
	covid_api_available = HttpSensor(
		task_id = "api_avaliability",
		method= "GET",
		http_conn_id= "kawalcorona_api",
		endpoint = "indonesia/provinsi",
		response_check = lambda response: "attributes" in response.text,
		poke_interval = 5,
		timeout = 20
	)

	covid_dataset_file = FileSensor(
		task_id = "covid_file",
		fs_conn_id = "data_path",
		filepath= "files",
		poke_interval= 5,
		timeout = 20
	)

	downloading_data = PythonOperator(
		task_id = 'downloading_data',
		python_callable= download_data
	)

	save_to_hdfs = BashOperator(
		task_id = "save_to_hdfs",
		bash_command = f"""
            	hdfs dfs -copyFromLocal /home/bayu_samudra_009/airflow/dags/files/covid_{str(datetime.date.today())}.json /user/bayu/
		"""
	)

	
	creating_table = HiveOperator(
        task_id="creating_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS covid_dataset(
        		tanggal DATE,
        		provinsi STRING,
        		positif INT,
        		sembuh INT,
        		meninggal INT,
				pertambahan_positif INT,
				pertambahan_sembuh INT,
				pertambahan_meninggal INT
        		)
        	ROW FORMAT DELIMITED
        	FIELDS TERMINATED BY ','
        	STORED AS TEXTFILE
        """
    )

	data_clean = SparkSubmitOperator(
		task_id = "data_cleaning",
		conn_id = "spark_default",
		application = "/home/bayu_samudra_009/airflow/dags/scripts/addingcolumn.py",
		verbose = False
	)

	slack_notif = SlackAPIPostOperator(
		task_id = "slack_notif",
		token = token,
		username = "airflow",
		text = "Task Done !!!",
		channel="#airflow-notif"
	)
