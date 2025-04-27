import os
from google.cloud import storage

# Define o caminho para o arquivo de credenciais
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'.json'

# Define o nome do bucket e o nome do blob de destino
bucket_name = 'epoca-bi'
destination_blob_name = 'base-bi/dados_rtb.csv'

# Inicia o cliente de armazenamento
storage_client = storage.Client()

# Define o caminho para o arquivo local
local_file_path = r'G:\Shared drives\Bases BI\dados_rtb.csv'

# Upload do arquivo para o Google Cloud Storage
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(local_file_path)


print("Upload concluído.")
