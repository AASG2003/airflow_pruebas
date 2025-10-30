"""
DAG 2: Pipeline de Procesamiento ETL
=====================================
Este DAG ejecuta el segundo pipeline que procesa los datos crudos extraídos,
los transforma, los carga a la base de datos OLTP y finalmente genera el 
archivo Parquet para el Data Warehouse.

Flujo:
1. Extraer y Normalizar datos desde R2 (CSV → DataFrames con pandas.melt())
2. Cargar a Staging en Postgres (DataFrames → tablas temporales)
3. Fusión Transaccional (Staging → Tablas de producción)
4. Generar Data Warehouse con DuckDB (Postgres → Parquet en R2)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator


# ============================================================================
# CONFIGURACIÓN DEL DAG
# ============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_procesamiento_etl',
    default_args=default_args,
    description='Pipeline de procesamiento ETL: Normalización, Carga OLTP y generación DWH',
    schedule=None,  # Trigger manual o desde DAG 1
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'procesamiento', 'oltp', 'dwh'],
)


# ============================================================================
# FUNCIONES DE TRANSFORMACIÓN Y CARGA
# ============================================================================

def extraer_y_normalizar_datos(**context):
    """
    Tarea 1: Extracción y Normalización
    ------------------------------------
    - Se conecta a R2 (S3-compatible)
    - Lee los archivos CSV crudos usando Pandas
    - Aplica pandas.melt() para des-pivotar columnas de notas
    - Normaliza el formato a DataFrames "largos"
    - Retorna los DataFrames normalizados para la siguiente tarea
    
    Salida esperada:
    - DataFrame de inscripciones normalizado
    - DataFrame de calificaciones normalizado
    """
    print("🔄 Iniciando extracción y normalización de datos desde R2...")
    
    # TODO: Conectar a R2 usando boto3 o s3fs
    # TODO: Leer CSVs con pd.read_csv()
    # TODO: Aplicar pandas.melt() para des-pivotar
    # TODO: Realizar limpieza y normalización de datos
    # TODO: Validar estructura de DataFrames
    
    print("✅ Datos extraídos y normalizados correctamente")
    
    # TODO: Retornar o pushear a XCom los DataFrames normalizados
    pass


def cargar_a_staging(**context):
    """
    Tarea 2: Carga a Staging en Postgres
    -------------------------------------
    - Recibe los DataFrames normalizados de la tarea anterior
    - Se conecta a la base de datos Postgres
    - Crea/trunca tablas de staging temporales
    - Carga masivamente los datos usando DataFrame.to_sql()
    
    Tablas de staging:
    - staging_inscripciones
    - staging_calificaciones
    """
    print("📤 Iniciando carga de datos a tablas de staging...")
    
    # TODO: Recuperar DataFrames de XCom
    # TODO: Conectar a Postgres usando SQLAlchemy
    # TODO: Crear tablas de staging si no existen
    # TODO: Cargar datos con to_sql(if_exists='replace')
    # TODO: Validar cantidad de registros cargados
    
    print("✅ Datos cargados a staging correctamente")
    pass
def fusion_staging_a_produccion(**context):
    pass

def generar_dwh_con_duckdb(**context):
    """
    Tarea 4: Generación del Data Warehouse
    ---------------------------------------
    - Utiliza DuckDB con la extensión postgres_scanner
    - Se conecta a Postgres (datos ya actualizados en producción)
    - Ejecuta consulta de desnormalización (JOIN de múltiples tablas)
    - Exporta el resultado como archivo Parquet
    - Sube el Parquet al bucket R2 en la carpeta warehouse/
    
    Salida:
    - warehouse/dwh_completo.parquet en R2
    """
    print("🦆 Iniciando generación de Data Warehouse con DuckDB...")
    
    # TODO: Inicializar DuckDB
    # TODO: Instalar y cargar extensión postgres_scanner
    # TODO: Conectar a Postgres desde DuckDB
    # TODO: Ejecutar consulta de desnormalización
    # TODO: Exportar resultado a Parquet local
    # TODO: Subir Parquet a R2 (warehouse/)
    # TODO: Limpiar archivos temporales
    
    print("✅ Data Warehouse generado y subido a R2 correctamente")
    pass


# ============================================================================
# DEFINICIÓN DE TAREAS
# ============================================================================

# Tarea de inicio
inicio = EmptyOperator(
    task_id='inicio_pipeline',
    dag=dag,
)

# FASE 1: Extracción y Normalización desde R2
tarea_extraer_normalizar = PythonOperator(
    task_id='extraer_y_normalizar_desde_r2',
    python_callable=extraer_y_normalizar_datos,
    dag=dag,
)

# FASE 2: Carga a Staging en Postgres
tarea_cargar_staging = PythonOperator(
    task_id='cargar_dataframes_a_staging',
    python_callable=cargar_a_staging,
    dag=dag,
)

# FASE 3: Fusión Transaccional (Staging → Producción)
tarea_fusion_transaccional = PythonOperator(
    task_id='fusion_staging_a_produccion',
    python_callable=fusion_staging_a_produccion,
    dag=dag,
)

# Checkpoint intermedio
checkpoint_oltp_actualizado = EmptyOperator(
    task_id='checkpoint_oltp_actualizado',
    dag=dag,
)

# FASE 4: Generación del Data Warehouse
tarea_generar_dwh = PythonOperator(
    task_id='generar_dwh_parquet_con_duckdb',
    python_callable=generar_dwh_con_duckdb,
    dag=dag,
)

# Tarea de finalización
fin = EmptyOperator(
    task_id='fin_pipeline',
    dag=dag,
)


# ============================================================================
# DEPENDENCIAS DEL PIPELINE
# ============================================================================

inicio >> tarea_extraer_normalizar
tarea_extraer_normalizar >> tarea_cargar_staging
tarea_cargar_staging >> tarea_fusion_transaccional
tarea_fusion_transaccional >> checkpoint_oltp_actualizado
checkpoint_oltp_actualizado >> tarea_generar_dwh
tarea_generar_dwh >> fin
