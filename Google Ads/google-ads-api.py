import csv
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.timestamp_pb2 import Timestamp
import os 
import time
from datetime import datetime, timedelta

from google.ads.googleads.client import GoogleAdsClient
client = GoogleAdsClient.load_from_storage("google-ads.yaml")


def getcampains(client):
    ga_service = client.get_service("GoogleAdsService")

    ga_service.login_customer_id = ""

    data = []
    customer_ids = ["", "", "", "", "", ""]

    # Subtraindo 3 horas do horário atual
    now = datetime.now() - timedelta(hours=3)

    # Ajustando "ontem" com 3 horas subtraídas
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    # Data "hoje" também com 3 horas subtraídas
    today = now.strftime('%Y-%m-%d')  
    # 4310311488 id não existe
    query = f"""
            SELECT 
                customer.id, campaign.name, 
                metrics.all_conversions_value, metrics.conversions,
                 metrics.clicks, metrics.conversions_value, 
                 metrics.cost_micros, metrics.ctr, customer.descriptive_name, 
                 segments.hour, metrics.impressions, metrics.average_cpc, 
                 metrics.cost_per_all_conversions, segments.date,
                 campaign_budget.recommended_budget_estimated_change_weekly_cost_micros 
            FROM 
                campaign
            WHERE 
                segments.date >= '{yesterday}' AND segments.date <= '{today}'"""
    for customer_id in customer_ids:
        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = customer_id
        search_request.query = query

        stream = ga_service.search_stream(search_request)

        for batch in stream:
            for row in batch.results:
                record = {
                    "customer_id": row.customer.id,
                    "campaign_name": row.campaign.name,
                    "all_conversions_value": row.metrics.all_conversions_value,
                    "conversions": row.metrics.conversions,
                    "clicks": row.metrics.clicks,
                    "conversions_value": row.metrics.conversions_value,
                    "cost_micros": row.metrics.cost_micros,
                    "ctr": row.metrics.ctr,
                    "customer_descriptive_name": row.customer.descriptive_name,
                    "date": row.segments.date,
                    "hour": row.segments.hour,
                    "impressions": row.metrics.impressions,
                    "average_cpc": row.metrics.average_cpc,
                    "cost_per_all_conversions": row.metrics.cost_per_all_conversions,
                    "recommended_budget_estimated_change_weekly_cost_micros": row.campaign_budget.recommended_budget_estimated_change_weekly_cost_micros,
                }
                data.append(record)

    df = pd.DataFrame(data)

    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df = df.astype({
    "customer_id": "string",
    "campaign_name": "string",
    "customer_descriptive_name": "string",
    "all_conversions_value": "float",
    "conversions": "float",
    "clicks": "int64",
    "conversions_value": "float",
    "cost_micros": "float",
    "ctr": "float",
    "impressions": "int64",
    "average_cpc": "float",
    "cost_per_all_conversions": "float",
    "recommended_budget_estimated_change_weekly_cost_micros": "string",
    "date": "object",
    "hour": "int64"
})
    df.to_csv('intraday-ads.csv', encoding='utf-8',index=False,quotechar='"')
    print({len(df)})



if __name__ == '__main__':
    getcampains(client)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.json'


bucket_name = 'storage'
destination_blob_name1 = 'Midia_Ads_API/intraday-ads.csv'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name1)
blob.upload_from_filename('intraday-ads.csv')

# Função para executar o Data Transfer no BigQuery
def start_data_transfer(transfer_config_name):
    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    now = datetime.utcnow()  # Corrigido para usar apenas datetime.utcnow()
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
transfer_config_2 = 'projects/epoca-230913/locations/us/transferConfigs/'

# Iniciar os Data Transfers 1
start_data_transfer(transfer_config_1)
print("O Data Transfers 1 - Google Ads API no BigQuery foi iniciado com sucesso.")


# Aguarda 2 minutos antes de iniciar o segundo Data Transfer (Faço isso para garantir que o fluxo que alimenta a base completa só rode após o fluxo intraday rode)
time.sleep(120)

# Iniciar o Data Transfer 2
start_data_transfer(transfer_config_2)
print("O Data Transfer 2 - Google Ads API Full no BigQuery foi iniciado com sucesso.")