# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 10:05:16 2024

@author: Facu
"""
import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from email import message
from email.mime.text import MIMEText
import airflow
import base64
import ssl
import smtplib






def import_transform_alphavantage_data(company,function, key,send, mensaje,ti):
    url = 'https://www.alphavantage.co/query?function='+function+'&symbol='+company+'&apikey='+key
    r = requests.get(url)
    data=r.json()
    
    dfc = pd.DataFrame(data["Time Series (Daily)"])
    dfc = dfc.T
    dfc['Company']=data['Meta Data']['2. Symbol']
    dfc=dfc.reset_index().rename(columns={"index":"Date","1. open":"Open Value","2. high":"Highest Value","3. low":"Lowest Value","4. close":"Close Value","5. volume":"Volume"})
    dfc = dfc.astype({'Date':'datetime64[ns]','Open Value':'float32','Highest Value':'float32','Lowest Value':'float32','Close Value':'float32','Volume':'int32'})
    mini = dfc[dfc['Company']==company].sort_values('Date',ascending=False).head(30)['Close Value'].min()
    maxi = dfc[dfc['Company']==company].sort_values('Date',ascending=False).head(30)['Close Value'].max()
    if maxi-(maxi*0.1)>mini:
        mensaje+='La acción de la companía '+company+' ha variado más de 10% en los últimos 30 días habiles\n'
        send=True
        print(mensaje)
    ti.xcom_push(key='send',value=send)
    ti.xcom_push(key='mensaje',value=mensaje)
    return dfc


def get_data_store(ti):
    df = pd.DataFrame()
    send=False
    mensaje=''
    companies=['YPF','GOOG','KO']
    for company in companies:
        df=pd.concat([import_transform_alphavantage_data(company,Variable.get("API_FUNCTION"),Variable.get("SECRET_API_KEY"), send, mensaje,ti),df])
        df['Timestamp'] = pd.Timestamp("now") 
        send=ti.xcom_pull(key='send',task_ids='download_data')
        mensaje=ti.xcom_pull(key='mensaje',task_ids='download_data')
        print(mensaje)
    conn = create_engine('postgresql://'+Variable.get("SECRET_DATABASE_USERNAME")+':'+Variable.get("SECRET_DATABASE_PASSWORD")+'@'+Variable.get("DATABASE_HOST")+':5439/'+Variable.get("DATABASE_NAME"))
    df.to_sql('alpha_vantage_shares', conn, index=False, if_exists='replace')
    ti.xcom_push(key='send',value=send)
    ti.xcom_push(key='mensaje',value=mensaje)
    
    
def enviar(ti):
    send=ti.xcom_pull(key='send',task_ids='download_data')
    mensaje=ti.xcom_pull(key='mensaje',task_ids='download_data')
    if not send:
        print('no enviar')
        return
    
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login(Variable.get("SECRET_MAIL"),Variable.get("SECRET_MAIL_PASSWORD")) # Cambia tu contraseña !!!!!!!!
        subject='Fechas fin del mundo'
        msg = MIMEText(mensaje)
        msg['From'] = Variable.get("SECRET_MAIL")
        msg['To'] = Variable.get("SECRET_MAIL_R")
        msg['Subject'] = subject
        x.sendmail(Variable.get("SECRET_MAIL"),Variable.get("SECRET_MAIL_R"),msg.as_string())
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')   


default_args={
    'owner': 'FacuVillafañe',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
     
}

apidata_dag = DAG(
        dag_id="Proyecto",
        default_args= default_args,
        description="DAG para consumir API y vaciar datos en Redshift",
        start_date=datetime(2024,4,20),
        schedule_interval='@daily' 
    )

task1 = BashOperator(task_id='start_task',
    bash_command='echo Start'
)

task2 = PythonOperator(
    task_id='download_data',
    python_callable=get_data_store,
    dag=apidata_dag,
)

task3 = PythonOperator(
    task_id='enviar',
    python_callable=enviar,
    dag=apidata_dag,
    provide_context=True
)


task1 >> task2 >> task3


