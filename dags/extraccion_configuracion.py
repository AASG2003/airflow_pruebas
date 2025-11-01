"""
DAG para la extracción automática de gradebooks de NetAcad - Con parámetros configurables
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os

# Argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

def setup_chrome_driver(**context):
    """Configurar el driver de Chrome y directorios necesarios"""
    # Obtener parámetros de configuración
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    # Configurar directorios
    airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    ciudad = conf.get('ciudad', 'default')
    download_dir = os.path.join(airflow_home, 'netacad_downloads', ciudad)
    os.makedirs(download_dir, exist_ok=True)
    
    # Configurar Chrome en modo headless
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--start-maximized')
    options.add_argument('--disable-blink-features=AutomationControlled')

    # Configurar descargas
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "plugins.always_open_pdf_externally": True,
        "download.open_pdf_in_system_reader": False
    }

    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_experimental_option("prefs", prefs)

    # Guardar configuración en XCom
    context['task_instance'].xcom_push(key='download_dir', value=download_dir)
    context['task_instance'].xcom_push(key='chrome_options', value=options)
    
    # Guardar parámetros de configuración
    context['task_instance'].xcom_push(key='username', value=conf.get('username', ''))
    context['task_instance'].xcom_push(key='password', value=conf.get('password', ''))
    context['task_instance'].xcom_push(key='search_content', value=conf.get('search_content', ''))
    context['task_instance'].xcom_push(key='ciudad', value=ciudad)
    
    print(f"✅ Configuración completada para ciudad: {ciudad}")
    print(f"📂 Directorio de descargas: {download_dir}")
    
    return "Configuración completada"

def extract_gradebooks(**context):
    """Ejecutar la extracción de gradebooks con parámetros configurables"""
    # Obtener configuración de XCom
    ti = context['task_instance']
    download_dir = ti.xcom_pull(key='download_dir')
    options = ti.xcom_pull(key='chrome_options')
    email = ti.xcom_pull(key='username')
    pass_text = ti.xcom_pull(key='password')
    search_content = ti.xcom_pull(key='search_content') or ""
    ciudad = ti.xcom_pull(key='ciudad')
    
    # Validar que se proporcionaron las credenciales
    if not email or not pass_text:
        raise ValueError("Se requieren username y password en la configuración del DAG run")
    
    print(f"🔐 Usuario: {email}")
    print(f"🔍 Búsqueda: '{search_content}'")
    print(f"🏙️ Ciudad: {ciudad}")
    
    driver = None
    try:
        driver = webdriver.Chrome(options=options)
        
        # 1. Abrir NetAcad
        driver.get("https://www.netacad.com/")

        # 2. Login
        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'login')]"))
        )
        login_button.click()

        username = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//input[@name='username']"))
        )
        username.send_keys(email)

        login_button = driver.find_element(By.XPATH, "//input[contains(@name, 'login')]")
        login_button.click()

        password = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//input[@name='password']"))
        )
        password.send_keys(pass_text)
        login_button = driver.find_element(By.XPATH, "//input[contains(@name, 'login')]")
        login_button.click()

        # 3. Buscar cursos
        searcher = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//input[@id='searchContent']"))
        )
        searcher.send_keys(search_content)

        search = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='search']"))
        )
        search.click()
        
        show_all = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//option[text()='100']"))
        )
        show_all.click()

        time.sleep(5)
        links = driver.find_elements(By.XPATH, "//a[contains(@class, 'btn--link')]")
        print(f"🔗 Se encontraron {len(links)} enlaces.")
        hrefs = [link.get_attribute("href") for link in links]
        
        # Guardar la cantidad de cursos encontrados
        ti.xcom_push(key='total_courses', value=len(hrefs))

        # 4. Procesar cada curso
        cursos_procesados = 0
        for href in hrefs:
            try:
                driver.get(href + "&tab=gradebook")
                
                # Descargar gradebook
                boton_descargas = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'iconDown')]"))
                )
                boton_descargas.click()

                botones = WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//button[contains(@class, 'dropdown-item')]"))
                )
                botones[0].click()

                # Esperar y aceptar la descarga
                boton_aceptar = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'btn--primary ')]"))
                )
                boton_aceptar.click()

                # Obtener título del curso
                curso = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, "//h1[contains(@class, 'course')]"))
                ).text.strip()

                # Esperar la descarga
                archivo_descargado = None
                archivos_iniciales = set(os.listdir(download_dir))
                start_time = time.time()

                while time.time() - start_time < 60:  # 60 segundos timeout
                    archivos_actuales = set(os.listdir(download_dir))
                    archivos_nuevos = [
                        os.path.join(download_dir, f) 
                        for f in archivos_actuales - archivos_iniciales 
                        if not f.endswith((".crdownload", ".tmp"))
                    ]
                    
                    if archivos_nuevos:
                        archivo_descargado = max(archivos_nuevos, key=os.path.getctime)
                        # Verificar descarga completa
                        tamaño_anterior = -1
                        while True:
                            tamaño_actual = os.path.getsize(archivo_descargado)
                            if tamaño_actual == tamaño_anterior:
                                break
                            tamaño_anterior = tamaño_actual
                            time.sleep(0.5)
                        break
                    time.sleep(1)

                if archivo_descargado:
                    # Renombrar archivo
                    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
                    extension = os.path.splitext(archivo_descargado)[1]
                    nuevo_nombre = f"{curso}_{fecha}{extension}"
                    ruta_nueva = os.path.join(download_dir, nuevo_nombre)
                    os.rename(archivo_descargado, ruta_nueva)
                    print(f"✅ Archivo guardado: {nuevo_nombre}")
                    cursos_procesados += 1
                
                time.sleep(5)
            except Exception as e:
                print(f"❌ Error procesando curso {href}: {str(e)}")
                continue

        # Guardar estadísticas finales
        ti.xcom_push(key='courses_processed', value=cursos_procesados)
        
        result_msg = f"Extracción completada: {cursos_procesados}/{len(hrefs)} cursos procesados para {ciudad}"
        print(f"✅ {result_msg}")
        return result_msg
    
    except Exception as e:
        error_msg = f"Error en la extracción: {str(e)}"
        print(error_msg)
        ti.xcom_push(key='error', value=error_msg)
        raise
    
    finally:
        if driver:
            driver.quit()

# Definición del DAG
with DAG(
    'netacad_gradebook_extraction_configurable',
    default_args=default_args,
    description='Extrae gradebooks de Cisco NetAcad con parámetros configurables',
    schedule_interval=None,  # Solo ejecución manual con configuración
    start_date=days_ago(1),
    catchup=False,
    tags=['netacad', 'gradebooks', 'configurable'],
) as dag:
    
    # Tarea de configuración
    setup_task = PythonOperator(
        task_id='setup_chrome_driver',
        python_callable=setup_chrome_driver,
        provide_context=True,
    )
    
    # Tarea de extracción
    extract_task = PythonOperator(
        task_id='extract_gradebooks',
        python_callable=extract_gradebooks,
        provide_context=True,
    )
    
    # Definir el orden de las tareas
    setup_task >> extract_task