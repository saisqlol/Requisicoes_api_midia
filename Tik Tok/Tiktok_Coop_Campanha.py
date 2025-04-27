import json
import requests
import pandas as pd
from urllib.parse import urlencode, urlunparse
from datetime import datetime, timedelta
import os
from google.cloud import storage

ACCESS_TOKEN = "xxx"
PATH = "/open_api/v1.3/report/integrated/get/"

def build_url(path, query=""):
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", "business-api.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get(json_str):
    """
    Send GET request
    :param json_str: Args in JSON format
    :return: Response in JSON format
    """
    args = json.loads(json_str)
    query_string = urlencode({k: v if isinstance(v, str) else json.dumps(v) for k, v in args.items()})
    url = build_url(PATH, query_string)
    headers = {
        "Access-Token": ACCESS_TOKEN,
    }
    rsp = requests.get(url, headers=headers)
    return rsp.json()

def tiktok(startDate, endDate):
    dftiktok = pd.DataFrame()
    advertiser_ids = ['7361809618651332625']
    service_type = 'AUCTION'
    data_level = "AUCTION_CAMPAIGN"
    dimensions_list = ['campaign_id', 'stat_time_day']
    dimensions = json.dumps(dimensions_list)
    metrics_list = [
        'campaign_name', 'spend', 'cpc', 'cpm', 'impressions', 'clicks', 'ctr', 'reach', 
        'cost_per_1000_reached', 'conversion', 'conversion_rate', 'currency', 'sales_lead', 
        'total_sales_lead_value', 'skan_sales_lead'
    ]
    metrics = json.dumps(metrics_list)
    page_size = 1000

    current_date = startDate
    while current_date <= endDate:
        for advertiser_id in advertiser_ids:
            my_args = {
                "metrics": metrics,
                "data_level": data_level,
                "end_date": current_date.strftime('%Y-%m-%d'),
                "page_size": page_size,
                "start_date": current_date.strftime('%Y-%m-%d'),
                "advertiser_id": advertiser_id,
                "service_type": service_type,
                "report_type": 'BASIC',
                "dimensions": dimensions
            }
            dados = get(json.dumps(my_args))

            if 'data' in dados and 'list' in dados['data']:
                data = dados['data']['list']
                if data:
                    stat_time_days = [item['dimensions']['stat_time_day'] for item in data]
                    impressions = [int(item['metrics']['impressions']) for item in data]
                    spend = [float(item['metrics']['spend']) for item in data]
                    cpc = [float(item['metrics'].get('cpc', 0)) for item in data]
                    cpm = [float(item['metrics'].get('cpm', 0)) for item in data]
                    clicks = [int(item['metrics'].get('clicks', 0)) for item in data]
                    ctr = [float(item['metrics'].get('ctr', 0)) for item in data]
                    reach = [int(item['metrics'].get('reach', 0)) for item in data]
                    cost_per_1000_reached = [float(item['metrics'].get('cost_per_1000_reached', 0)) for item in data]
                    conversion = [int(item['metrics'].get('conversion', 0)) for item in data]
                    conversion_rate = [float(item['metrics'].get('conversion_rate', 0)) for item in data]
                    currency = [item['metrics'].get('currency', '') for item in data]
                    sales_lead = [int(item['metrics'].get('sales_lead', 0)) for item in data]
                    total_sales_lead_value = [float(item['metrics'].get('total_sales_lead_value', 0)) for item in data]
                    skan_sales_lead = [int(item['metrics'].get('skan_sales_lead', 0)) for item in data]
                    campaign_names = [item['metrics']['campaign_name'] for item in data]

                    df = pd.DataFrame({
                        'stat_time_day': stat_time_days,
                        'impressions': impressions,
                        'spend': spend,
                        'cpc': cpc,
                        'cpm': cpm,
                        'clicks': clicks,
                        'ctr': ctr,
                        'reach': reach,
                        'cost_per_1000_reached': cost_per_1000_reached,
                        'conversion': conversion,
                        'conversion_rate': conversion_rate,
                        'currency': currency,
                        'sales_lead': sales_lead,
                        'total_sales_lead_value': total_sales_lead_value,
                        'skan_sales_lead': skan_sales_lead,
                        'campaign_name': campaign_names
                    })

                    df = df[((df['impressions'] != 0) | (df['spend'] != 0))]
                    df['stat_time_day'] = pd.to_datetime(df['stat_time_day'])
                    df['date'] = df['stat_time_day'].dt.strftime('%Y-%m-%d')
                    df = df.astype({
                        'impressions': int,
                        'spend': float,
                        'cpc': float,
                        'cpm': float,
                        'clicks': int,
                        'ctr': float,
                        'reach': int,
                        'conversion': int,
                        'campaign_name': str
                    })
                    df['ROAS'] = round(df['conversion']/df['spend'],2).astype(float)
                    dftiktok = pd.concat([dftiktok, df[[
                        'date', 'campaign_name', 'impressions','spend','clicks','reach','cpc','cpm','ctr','conversion','ROAS'
                    ]]], ignore_index=True)

        current_date += timedelta(days=1)

    return dftiktok

# Definindo o intervalo de datas
endDate = datetime.today()
startDate = endDate - timedelta(days=40)

df_final = tiktok(startDate, endDate)

# Salvar DataFrame em um arquivo CSV
nome_arquivo_csv = 'Tik_Tok_Coop_Campanhas.csv'
df_final.to_csv(nome_arquivo_csv, index=False)
print(f'Dados salvos no arquivo: {nome_arquivo_csv}')

# Subir o arquivo no GCP - Bucket 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.json'

bucket_name = 'epoca-storage'
destination_blob_name = 'Midia_Ads_API/Tik_Tok_Coop_Campanhas.csv'

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(nome_arquivo_csv)
print("O arquivo subiu com sucesso!!")