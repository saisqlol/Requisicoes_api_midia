### Transferência para dados mensais

import requests

url = "https://api.similarweb.com/batch/v4/request-report"

payload = {
    "report_name": "Requisicao_Active_users",
    "delivery_information": { "response_format": "csv" },
    "report_query": { "tables": [
            {
                "vtable": "traffic_and_engagement",
                "granularity": "monthly",
                "latest": True,
                "filters": {
                    "domains": ["epocacosmeticos.com.br","belezanaweb.com.br","sephora.com.br"],
                    "countries": ["BR"],
                    "include_subdomains": True
                },
                "metrics": ["mobile_visits", "desktop_visits"],
                "start_date": "2024-01"
            }
        ] }
}
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "api-key": "xxxx"
}

response = requests.post(url, json=payload, headers=headers)


content= response.content.decode("utf-8")
comeco = content.find('report_id":"')+len('report_id":"')
fim = content.find('",',comeco)
report_id = content[comeco:fim]

import os
import time

url = f"https://api.similarweb.com/v3/batch/request-status/{report_id}"

payload = {}
headers = {
    'api-key': "xxx"
}

# Loop para verificar o status
while True:
    response = requests.get(url, headers=headers, data=payload)

    if response.status_code == 200:
        response_data = response.json()
        status = response_data.get("status")
        print(f"Status atual: {status}")

        if status == "completed":
            download_url = response_data.get("download_url")
            if download_url:
                # Fazer o download do arquivo
                file_response = requests.get(download_url, stream=True)

                if file_response.status_code == 200:
                    # Definir o caminho para salvar o arquivo
                    save_path = os.path.join("/home/ubuntu/dados/similiar-api", "similarweb.csv")

                    with open(save_path, "wb") as file:
                        for chunk in file_response.iter_content(chunk_size=8192):
                            file.write(chunk)

                    print(f"Arquivo salvo com sucesso em: {save_path}")
                else:
                    print(f"Erro ao baixar o arquivo. Código de status: {file_response.status_code}")
            else:
                print("URL de download não encontrada na resposta.")
            break  # Sair do loop após concluir o download
        elif status in ["pending", "processing"]:
            print("Relatório ainda em processamento. Tentando novamente em 15 segundos...")
            time.sleep(15)  # Esperar 15 segundos antes de tentar novamente
        else:
            print(f"Status inesperado: {status}. Finalizando.")
            break
    else:
        print(f"Erro na requisição. Código de status: {response.status_code}")
        break

import pandas as pd

df=pd.read_csv('similarweb.csv',dtype=str)
df['desktop_visits'] = df['desktop_visits'].astype(float)
df['mobile_visits'] = df['mobile_visits'].astype(float)

pwd =  "/home/ubuntu/dados/similiar-api/similiarweb_mes.csv"
df.to_csv(pwd, index=False)


from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'chave.json'

bucket_name = 'epoca-storage'
destination_blob_name1 = 'Midia_Ads_API/similiarweb_mes.csv'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name1)
blob.upload_from_filename(pwd)


### Transferência para dados diários

from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'chave.json'

client = bigquery.Client()

query = """
    SELECT MAX(date) Data FROM `epoca-230913.5264682.Similiar_Web_Dia`
"""
query_job = client.query(query)
max_data = query_job.result().to_dataframe()
data_maxima = max_data['Data'].iloc[0]

data_maxima_str = str(data_maxima)


url = "https://api.similarweb.com/batch/v4/request-report"

payload = {
    "report_name": "Requisicao_Active_users",
    "delivery_information": { "response_format": "csv" },
    "report_query": { "tables": [
            {
                "vtable": "traffic_and_engagement",
                "granularity": "daily",
                "latest": True,
                "filters": {
                    "domains": ["epocacosmeticos.com.br","belezanaweb.com.br","sephora.com.br"],
                    "countries": ["BR"],
                    "include_subdomains": True
                },
                "metrics": ["mobile_visits", "desktop_visits"],
                "start_date": data_maxima_str
            }
        ] }
}
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "api-key": "xxx"
}

response = requests.post(url, json=payload, headers=headers)

content= response.content.decode("utf-8")
comeco = content.find('report_id":"')+len('report_id":"')
fim = content.find('",',comeco)
report_id = content[comeco:fim]

url = f"https://api.similarweb.com/v3/batch/request-status/{report_id}"

payload = {}
headers = {
    'api-key': "xxx"
}

# Loop para verificar o status
while True:
    response = requests.get(url, headers=headers, data=payload)

    if response.status_code == 200:
        response_data = response.json()
        status = response_data.get("status")
        print(f"Status atual: {status}")

        if status == "completed":
            download_url = response_data.get("download_url")
            if download_url:
                # Fazer o download do arquivo
                file_response = requests.get(download_url, stream=True)

                if file_response.status_code == 200:
                    # Definir o caminho para salvar o arquivo
                    save_path = os.path.join("/home/ubuntu/dados/similiar-api", "similarweb_daily.csv")

                    with open(save_path, "wb") as file:
                        for chunk in file_response.iter_content(chunk_size=8192):
                            file.write(chunk)

                    print(f"Arquivo salvo com sucesso em: {save_path}")
                else:
                    print(f"Erro ao baixar o arquivo. Código de status: {file_response.status_code}")
            else:
                print("URL de download não encontrada na resposta.")
            break  # Sair do loop após concluir o download
        elif status in ["pending", "processing"]:
            print("Relatório ainda em processamento. Tentando novamente em 15 segundos...")
            time.sleep(15)  # Esperar 15 segundos antes de tentar novamente
        else:
            print(f"Status inesperado: {status}. Finalizando.")
            break
    else:
        print(f"Erro na requisição. Código de status: {response.status_code}")
        break


### Fazendo envio dos dados ao GCP e rodando a consulta programada

import pandas as pd
from google.cloud import storage

df = pd.read_csv('similarweb_daily.csv', dtype=str)
df['desktop_visits'] = df['desktop_visits'].astype(float)
df['mobile_visits'] = df['mobile_visits'].astype(float)

pwd = "/home/ubuntu/dados/similiar-api/similiarweb_dia.csv"
df.to_csv(pwd, index=False)

# Obter a data máxima do DataFrame
max_data_df = pd.to_datetime(df['date']).max()
max_data_str = max_data_df.strftime('%Y-%m-%d')


# Verificação
if max_data_str == data_maxima_str:
    print(f"As datas são iguais ({max_data_str}). Upload cancelado.")
else:
    print(f"As datas são diferentes. Fazendo o upload do arquivo para o bucket.")

    # Configurar o Google Cloud Storage
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'chave.json'

    bucket_name = 'epoca-storage'
    destination_blob_name1 = 'Midia_Ads_API/similiarweb_dia.csv'

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name1)

    # Fazer o upload do arquivo
    blob.upload_from_filename(pwd)
    print("Upload concluído com sucesso.")

    time.sleep(15)

    from google.cloud import bigquery_datatransfer_v1
    from google.protobuf.timestamp_pb2 import Timestamp
    import time
    import datetime

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


    transfer_config_1 = 'projects/epoca-230913/locations/us/transferConfigs/'

# Iniciar os Data Transfers 1
start_data_transfer(transfer_config_1)
print("O Data Transfers 1 - Similiar Web Mes/Dia no BigQuery foi iniciado com sucesso.")