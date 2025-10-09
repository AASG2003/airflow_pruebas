from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime
import requests
import pandas as pd
from urllib.parse import quote

# ==================== Config ====================

# Base del API: configurable por variable de entorno. En contenedores, host.docker.internal
# apunta a tu máquina (Windows/Mac). Puedes sobreescribir API_BASE_URL en el entorno si lo necesitas.
API_BASE_URL = ("http://host.docker.internal:8000").rstrip("/")

# ==================== Funciones ====================

def login_and_get_token():
    context = get_current_context()
    login_url = f"{API_BASE_URL}/api/pg/token/login"
    payload = {"username": "correo@ejemplo.com", "password": "prueba1234"}
    headers = {"accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}

    resp = requests.post(login_url, data=payload, headers=headers, timeout=15)
    resp.raise_for_status()
    token = resp.json()["token"]

    # Guardar token en XCom
    context["ti"].xcom_push(key="token", value=token)
    print("Token obtenido:", token)

def get_csv_path():
    context = get_current_context()
    token = context["ti"].xcom_pull(key="token", task_ids="get_token")
    files_url = f"{API_BASE_URL}/api/pg/files/1"
    headers = {"accept": "application/json", "Authorization": token}

    resp = requests.get(files_url, headers=headers, timeout=15)
    resp.raise_for_status()
    path = resp.json()["path"]

    # Devolver a como estaba: mantener host/esquema originales (S3 o presigned URL)
    # y sólo URL-encodear el nombre del archivo
    base_url, filename = path.rsplit("/", 1)
    safe_url = f"{base_url}/{quote(filename)}"

    context["ti"].xcom_push(key="csv_path", value=safe_url)
    print("CSV path seguro:", safe_url)

def process_and_send():
    context = get_current_context()
    csv_path = context["ti"].xcom_pull(key="csv_path", task_ids="get_csv")
    endpoint_url = f"{API_BASE_URL}/api/mongo/courses/"
    headers_post = {"accept": "application/json", "Content-Type": "application/json"}

    # Descargar CSV
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(csv_path, headers=headers, timeout=30)
    resp.raise_for_status()

    # Guardar temporalmente
    tmp_file = "/tmp/temp.csv"
    with open(tmp_file, "wb") as f:
        f.write(resp.content)

    # Leer CSV con pandas
    df = pd.read_csv(tmp_file)
    df.columns = [
        "Carrera","Semestre","Materia","Paralelo","Docente","Sede Académica",
        "Academia","Curso","Forma","Código Curso","Inscritos","Aprobados",
        "No Iniciados","Reprobados","Porcentaje"
    ]

    # Reemplazar NaN
    df.fillna({
        "Inscritos": 0,
        "Aprobados": 0,
        "No Iniciados": 0,
        "Reprobados": 0,
        "Porcentaje": 0.0
    }, inplace=True)
    df.fillna("", inplace=True)  # columnas de texto

    # Funciones seguras
    def safe_int(value):
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    def safe_float(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def safe_str(value):
        if pd.isna(value):
            return ""
        return str(value)

    # Enviar fila por fila
    for _, row in df.iterrows():
        data = {
            "carrera": safe_str(row.get("Carrera")),
            "semestre": safe_str(row.get("Semestre")),
            "materia": safe_str(row.get("Materia")),
            "paralelo": safe_str(row.get("Paralelo")),
            "docente": safe_str(row.get("Docente")),
            "sede_academica": safe_str(row.get("Sede Académica")),
            "academia": safe_str(row.get("Academia")),
            "curso": safe_str(row.get("Curso")),
            "forma": safe_str(row.get("Forma")),
            "codigo_curso": safe_str(row.get("Código Curso")),
            "inscritos": safe_int(row.get("Inscritos")),
            "aprobados": safe_int(row.get("Aprobados")),
            "no_iniciados": safe_int(row.get("No Iniciados")),
            "reprobados": safe_int(row.get("Reprobados")),
            "porcentaje": safe_float(row.get("Porcentaje"))
        }
        r = requests.post(endpoint_url, headers=headers_post, json=data, timeout=15)
        if r.status_code != 200:
            print(f"Error en {data['codigo_curso']}: {r.text}")

# ==================== DAG ====================

with DAG(
    dag_id="etl_csv_to_api",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="get_token",
        python_callable=login_and_get_token,
    )

    t2 = PythonOperator(
        task_id="get_csv",
        python_callable=get_csv_path,
    )

    t3 = PythonOperator(
        task_id="process_and_send",
        python_callable=process_and_send,
    )

    t1 >> t2 >> t3
