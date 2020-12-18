# Building INDONESIA COVID DATASET Workflow  

This Project are set on Google Cloud Platform VM
this workflow will check the availability of the api where we send get requests, then if we get expected response it will go to the next task. Downloading_data is the next task it write specific part of the response we got as a json file then save that json to hdfs. After we have the json file in hdfs we create hive table where we will save the file as a structured query. before insert that file we use spark to get the increasing value of positif, sembuh, and meninggal each day

# Getting Started
. install airflow on your GCP cluster
  run airflow initdb, airflow scheduler, airflow webserver
. create 3 conn_id on airflow web ui
  conn id : kawalcorona_api, conn Type: HTTP, Host: http://api.kawalcorona.com/
  conn id: data_path, conn Type: File(path), Extra: {"path": "~/airflow/dags/files"}
  conn id: hive_conn, conn Type: Hive Server 2 Thrift, host: hive-server, port: 10000
