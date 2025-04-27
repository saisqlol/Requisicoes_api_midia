import requests
import pandas as pd
import io
import os
import urllib3
import json
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.timestamp_pb2 import Timestamp
import time


urllib3.disable_warnings()


#------------------------------------ Pegando acess token -----------------------------------------------#

# Anotações:

# URL do endpoint para obter o token:
token_url = "https://api.criteo.com/oauth2/token"


application_id = ""
client_id = ""
client_secret = ""
grant_type = "client_credentials"

# Parâmetros
payload = {
    "application_id": application_id,
    "client_id": client_id,
    "client_secret": client_secret,
    "grant_type": grant_type
}

#request
response = requests.post(token_url, data=payload, verify=False)
#token
access_token = response.json()['access_token']
print(access_token)

#------------------------------------ Pegando acess token -----------------------------------------------#

#Range com base no formato ISO8601 e tirando 3 horas por causa do fuso horário da VM
today = datetime.now() - timedelta(hours=3)
start_date = (today - timedelta(days=1)).isoformat()
end_date = today.isoformat()

#Parâmetros
payload = {
    "timezone": "UTC",
    "dimensions": ["CampaignId", "AdsetId", "Device", "Day","Category","Hour"],
    "currency": "BRL",
    "format": "json",
    "metrics": ["Clicks","Cpc","Audience","Displays","ECpm","Reach","AdvertiserCost"],
    "startDate": start_date,
    "endDate": end_date
}


payload_json = json.dumps(payload)


url = "https://api.criteo.com/2024-01/statistics/report"
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Bearer {access_token}"
}

response = requests.post(url, data=payload_json, headers=headers, verify=False)


if response.status_code == 200:

    data = response.json()


    campaign_data = data["Rows"]


    report_campanha_clicks = pd.DataFrame(campaign_data)


    report_campanha_clicks
else:
    print("Erro ao fazer a solicitação:", response.status_code)

# Ajustar o campo 'Hour' para conter apenas a hora
report_campanha_clicks['Hour'] = pd.to_datetime(report_campanha_clicks['Hour']).dt.hour


report_campanha_clicks = report_campanha_clicks.astype({
    'CampaignId': 'string',
    'Campaign': 'string',
    'AdsetId': 'string',
    'Adset': 'string',
    'Device': 'string',
    'Day': 'datetime64[ns]',  
    'CategoryId': 'string',
    'Category': 'string',
    'Hour': 'int64',           
    'Currency': 'string',
    'Clicks': 'int64',
    'Cpc': 'float',
    'Audience': 'int64',
    'Displays': 'int64',
    'ECpm': 'float',
    'Reach': 'float',
    'AdvertiserCost': 'float'
})

report_campanha_clicks.to_csv('intraday_criteo.csv',index=False,encoding='utf-8',quotechar='"')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'chave.json'

bucket_name = 'storage'
destination_blob_name1 = 'Midia_Ads_API/intraday_criteo.csv'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name1)
blob.upload_from_filename('intraday_criteo.csv')


time.sleep(50)

# Função para executar o Data Transfer no BigQuery
def start_data_transfer(transfer_config_name):
    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    now = datetime.utcnow() - timedelta(hours=3)
    seconds = int(now.timestamp())
    # Definindo o período de execução manual
    run_time = Timestamp(seconds=seconds)
    # Disparar o job de transferência
    request = bigquery_datatransfer_v1.StartManualTransferRunsRequest(
        parent=transfer_config_name,
        requested_run_time=run_time,
    )
    response = client.start_manual_transfer_runs(request=request)


transfer_config_1 = 'projects/epoca-230913/locations/us/transferConfigs/'
