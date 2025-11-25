# test_bq_auth.py
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
LOCATION   = os.environ["BQ_LOCATION"]
KEY_JSON   = os.environ["GCP_KEY_JSON_BIG_QUERY"]

# Carrega EXPLICITAMENTE o service account do .env
creds = service_account.Credentials.from_service_account_file(
    KEY_JSON,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

print(">> Usando credenciais:")
print("   tipo         :", type(creds).__name__)
print("   SA e-mail    :", getattr(creds, 'service_account_email', 'N/A'))
print("   projeto      :", PROJECT_ID)
print("   localização  :", LOCATION)
print()

# Cria o client com essas credenciais
bq = bigquery.Client(project=PROJECT_ID, credentials=creds, location=LOCATION)

# Teste 1: lista datasets (não cria job)
print(">> Listando datasets do projeto…")
for ds in bq.list_datasets(project=PROJECT_ID):
    print("   -", ds.dataset_id)
print()

# Teste 2: tenta rodar um job simples (SELECT 1)
print(">> Rodando SELECT 1 para testar bigquery.jobs.create …")
sql = "SELECT 1 AS ok"
rows = list(bq.query(sql).result())   # se não tiver permissão, estoura aqui
print("   Resultado:", rows[0]["ok"])
print(">> OK: seu service account tem permissão para criar jobs.")
