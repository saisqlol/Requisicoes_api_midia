import requests
import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

hoje = datetime.now() - timedelta(hours=3)
data_ini = hoje - timedelta(days=1)
data_fim = hoje

load_dotenv()

url = os.getenv('url')

headers = {
    "Authorization": os.getenv('Authorization')
}



consolidado = pd.DataFrame()


current_date = data_ini
while current_date <= data_fim:
    
    date_str = current_date.strftime("%Y-%m-%d")
    
    
    params = {
        "kpis": "installs,sessions,loyal_users,cost,revenue,arpu_ltv,roi,event_counter_af_purchase,activity_revenue,activity_sessions,activity_event_counter_af_purchase,activity_average_unique_users_af_purchase",
        "groupings": "pid,c,app_id",
        "format": "json",
        "to": date_str,
        "from": date_str,
        "currency": "preferred",
        "timezone": "preferred"
    }
    
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        
        data = response.json()
        
       
        df = pd.DataFrame(data)
        
        
        df["Event_time"] = date_str
        
        
        consolidado = pd.concat([consolidado, df], ignore_index=True)
    else:
        print(f"Erro na requisição para {date_str}: {response.status_code} - {response.text}")
    
    
    current_date += timedelta(days=1)


consolidado.to_csv("appsflyer_installs.csv", index=False)

import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/home/ubuntu/dados/chave_bq/chave.json'

bucket_name = 'epoca-storage'
destination_blob_name1 = 'Midia_Ads_API/appsflyer_installs.csv'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name1)
blob.upload_from_filename('appsflyer_installs.csv')



from google.cloud import bigquery_datatransfer_v1
from google.protobuf.timestamp_pb2 import Timestamp
import time
import datetime

time.sleep(10) # Espera 10 segundos para o arquivo subir no Bucket

 # Função para executar o Data Transfer no BigQuery
def start_data_transfer(transfer_config_name):
    client = bigquery_datatransfer_v1.DataTransferServiceClient()

    now = datetime.datetime.utcnow()
    seconds = int(now.timestamp())
    
    # Definindo o período de execução manual
    run_time = Timestamp(seconds=seconds)

    # Disparar o job de transferência
    request = bigquery_datatransfer_v1.StartManualTransferRunsRequest(
        parent=transfer_config_name,
        requested_run_time=run_time,
    )
    
    response = client.start_manual_transfer_runs(request=request)
    print(f"Data Transfer {transfer_config_name} iniciado com sucesso.")


transfer_config_1 = 'projects/epoca-230913/locations/us/transferConfigs/678d8420-0000-2770-81ec-f4f5e803b9e4'

# Iniciar os Data Transfers 1
start_data_transfer(transfer_config_1)
print("O Data Transfers 1 - Appsflyer API Master Agg no BigQuery foi iniciado com sucesso.")

