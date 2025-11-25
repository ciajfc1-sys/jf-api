# api.py — Flask + BigQuery (usando VIEWS)
import os
from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import json


# ========= .env (na MESMA pasta do api.py) =========
# GCP_PROJECT_ID=bd-cia-jf-citrus
# BQ_LOCATION=southamerica-east1
# BQ_DATASET=jf_prod
# GCP_KEY_JSON=C:\Users\gbolaina\OneDrive - JF\Trabalho\Conexão Banco\bd-cia-jf-citrus-68863a3faf0f.json
load_dotenv()

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
LOCATION   = os.environ["BQ_LOCATION"]
DATASET    = os.environ["BQ_DATASET"]
#KEY_JSON   = os.environ["GCP_KEY_JSON_BIG_QUERY"]

# ========= BigQuery =========
#creds = service_account.Credentials.from_service_account_file(KEY_JSON)
#bq    = bigquery.Client(project=PROJECT_ID, credentials=creds, location=LOCATION)

# Caminho do JSON (modo local) e/ou conteúdo do JSON (modo cloud)
KEY_JSON_PATH = os.environ.get("GCP_KEY_JSON_BIG_QUERY")
SERVICE_ACCOUNT_JSON = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")

# ========= BigQuery =========
if SERVICE_ACCOUNT_JSON:
    # Produção (Render): credenciais vêm em uma env var com o JSON inteiro
    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(info)
elif KEY_JSON_PATH:
    # Desenvolvimento local: continua usando o caminho do arquivo no Windows
    creds = service_account.Credentials.from_service_account_file(KEY_JSON_PATH)
else:
    raise RuntimeError(
        "Configure GCP_SERVICE_ACCOUNT_JSON (JSON inteiro) "
        "ou GCP_KEY_JSON_BIG_QUERY (caminho do arquivo)."
    )

bq = bigquery.Client(project=PROJECT_ID, credentials=creds, location=LOCATION)


# ========= KML =========
# Mantém o mesmo esquema que você já usou: DADOS\kml\...
KML_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "kml")

def fq(table_or_view: str) -> str:
    """Retorna nome totalmente qualificado com crase para BigQuery."""
    return f"`{PROJECT_ID}.{DATASET}.{table_or_view}`"

# ========= Flask =========
app = Flask(__name__)
# CORS aberto no dev. Se quiser, especifique origins=["http://127.0.0.1:5173","http://localhost:5173"]
CORS(app)

# ---------------------------------------------------
# Saúde
# ---------------------------------------------------
@app.get("/healthz")
def healthz():
    return "ok"

# ---------------------------------------------------
# KML estático
# GET /kml/JF_GERAL_LINHAS.kml
# GET /kml/CENTROIDE.kml
# ---------------------------------------------------
@app.get("/kml/<path:filename>")
def kml_static(filename):
    fpath = os.path.join(KML_DIR, filename)
    if not os.path.isfile(fpath):
        abort(404)
    return send_from_directory(
        KML_DIR,
        filename,
        mimetype="application/vnd.google-earth.kml+xml",
        as_attachment=False
    )

# ---------------------------------------------------
# TOP 10 genérico (para a aba "Tabelas" e "Preços")
# GET /api/top10?table=FAT_ARMADILHA_PSILIDEO
# ---------------------------------------------------
ALLOWED_TABLES = {
    "FAT_ARMADILHA_PSILIDEO",
    "DIM_EMPERP",
    "DIM_TALHAO_PIMS",
    # 👇 novas views de preços
    "V_INDICADORES_MERCADO_DIARIO",
    "indicadores_mercado_ultima",
}

@app.get("/api/top10")
def api_top10():
    table = (request.args.get("table") or "").strip()
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Tabela não permitida ou ausente"}), 400
    try:
        sql  = f"SELECT * FROM {fq(table)} LIMIT 10"
        rows = list(bq.query(sql).result())
        if not rows:
            return jsonify({"columns": [], "rows": []})
        cols = list(rows[0].keys())
        data = [{c: r[c] for c in cols} for r in rows]
        return jsonify({"columns": cols, "rows": data})
    except Exception as e:
        # devolve erro detalhado para você ver no front/console durante dev
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------
# ÚLTIMA CAPTURA por CHAVE (popup do mapa)
# usa a VIEW: vw_psilideo_ultima_captura
# GET /api/ultima_captura_psilideo?chave=XXXX
# retorna: {"sk_data":"YYYY-MM-DD","qtd":N}
# ---------------------------------------------------
@app.get("/api/ultima_captura_psilideo")
def api_ultima():
    chave = (request.args.get("chave") or "").strip()
    if not chave:
        return jsonify({"error": "falta parâmetro chave"}), 400
    try:
        sql = f"""
        SELECT
          FORMAT_DATE('%Y-%m-%d', sk_data) AS sk_data,
          qtd
        FROM {fq("vw_psilideo_ultima_captura")}
        WHERE chave = @ch
        """
        job = bq.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("ch", "STRING", chave)]
            ),
        )
        rows = list(job.result())
        if not rows:
            return jsonify({"sk_data": None, "qtd": 0})
        r = rows[0]
        return jsonify({"sk_data": r["sk_data"], "qtd": r["qtd"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------
# Série semanal (sex→qui) por CHAVE (gráfico)
# usa a VIEW: vw_psilideo_semana
# GET /api/serie_psilideo?chave=XXXX[&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD]
# retorna: {"series":[{"ano":YYYY,"semana":WW,"qtd":N}, ...]}
# ---------------------------------------------------
@app.get("/api/serie_psilideo")
def api_serie():
    chave = (request.args.get("chave") or "").strip()
    start = (request.args.get("start_date") or "").strip()  # opcional
    end   = (request.args.get("end_date") or "").strip()    # opcional

    if not chave:
        return jsonify({"series": []})

    try:
        sql = f"""
        SELECT ano, semana, qtd
        FROM {fq("vw_psilideo_semana")}
        WHERE chave = @ch
          AND (@s IS NULL OR semana_inicio_sexta >= @s)
          AND (@e IS NULL OR semana_inicio_sexta <= @e)
        ORDER BY ano, semana
        """
        params = [
            bigquery.ScalarQueryParameter("ch", "STRING", chave),
            bigquery.ScalarQueryParameter("s", "DATE", start if start else None),
            bigquery.ScalarQueryParameter("e", "DATE", end   if end   else None),
        ]
        job = bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
        rows = list(job.result())
        series = [{"ano": r["ano"], "semana": r["semana"], "qtd": r["qtd"]} for r in rows]
        return jsonify({"series": series})
    except Exception as e:
        return jsonify({"error": str(e), "series": []}), 500
    
# ---------------------------------------------------
# Endpoint genérico SEM LIMIT
# GET /api/table?table=V_INDICADORES_MERCADO_DIARIO
# ---------------------------------------------------
@app.get("/api/table")
def api_table():
    table = (request.args.get("table") or "").strip()
    if table not in ALLOWED_TABLES:
        return jsonify({"error": "Tabela não permitida ou ausente"}), 400

    try:
        sql = f"SELECT * FROM {fq(table)}"
        rows = list(bq.query(sql).result())

        if not rows:
            return jsonify({"columns": [], "rows": []})

        cols = list(rows[0].keys())
        data = [{c: r[c] for c in cols} for r in rows]

        return jsonify({"columns": cols, "rows": data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
if __name__ == "__main__":
    # Rode:  cd "C:\Users\gbolaina\OneDrive - JF\Trabalho\DADOS\api"
    #        python .\api.py
    # Teste: http://127.0.0.1:5000/healthz  -> ok
    app.run(host="127.0.0.1", port=5000, debug=True)
