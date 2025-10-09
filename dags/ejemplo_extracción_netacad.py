from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import os

# Configuración por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Parámetros por defecto que pueden ser sobreescritos vía API
default_params = {
    # Credenciales NetAcad
    'email': 'asoriag@univalle.edu',  # Email por defecto
    'password': "B.e.T.o.2003",  # La contraseña debe ser proporcionada vía API
    'curso_filter': '',  # Filtro opcional para cursos
    'download_dir': '/opt/airflow/netacad_downloads/temp',  # Directorio de descargas
    
    # Credenciales API
    'api_url': ("http://host.docker.internal:8000").rstrip("/"),  # URL base de la API
    'api_username': 'correo@ejemplo.com',  # Usuario para la API
    'api_password': 'prueba1234'  # Contraseña para la API (requerido)
}

def descargar_cursos_netacad(**context):
    """
    Función que realiza el web scraping de NetAcad usando Firefox
    """
    # Importar Selenium solo cuando se ejecute la tarea
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.firefox.service import Service
    
    # Obtener parámetros del contexto
    params = context['dag_run'].conf if context['dag_run'] and context['dag_run'].conf else {}
    
    # Combinar con valores por defecto
    email = params.get('email', default_params['email'])
    password = params.get('password', default_params['password'])
    curso_filter = params.get('curso_filter', default_params['curso_filter'])
    download_dir = params.get('download_dir', default_params['download_dir'])
    
    # Validar parámetros requeridos
    if not password:
        raise ValueError("Se requiere la contraseña en la configuración del DAG")
    
    # Configuración de directorios
    download_dir = os.path.abspath(download_dir)
    os.makedirs(download_dir, exist_ok=True)
    
    # Configurar Firefox en modo headless
    options = FirefoxOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    
    # Configurar descargas para Firefox
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.dir", download_dir)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", 
                          "application/pdf,application/vnd.ms-excel,text/csv,application/zip")
    options.set_preference("browser.download.useDownloadDir", True)
    options.set_preference("pdfjs.disabled", True)
    link = "https://www.netacad.com/"
    search_content = curso_filter
    
    driver = None
    
    try:
        # Inicializar Firefox WebDriver
        driver = webdriver.Firefox(options=options)
        
        print("🚀 Iniciando proceso de scraping en NetAcad...")
        print(f"📧 Usando cuenta: {email}")
        if curso_filter:
            print(f"🔍 Filtrando por: {curso_filter}")
        
        # 1. Abrir NetAcad
        driver.get(link)
        
        # 2. Login
        print("🔐 Realizando login...")
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
        
        password_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//input[@name='password']"))
        )
        password_field.send_keys(password)
        
        login_button = driver.find_element(By.XPATH, "//input[contains(@name, 'login')]")
        login_button.click()
        
        # Verificar login exitoso
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@id='searchContent']"))
            )
            print("✅ Login exitoso")
        except:
            raise Exception("❌ Error en el login: Verifica las credenciales proporcionadas")
        
        print("✅ Login exitoso")
        
        # 3. Búsqueda
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
        
        # 4. Obtener enlaces
        time.sleep(5)
        links = driver.find_elements(By.XPATH, "//a[contains(@class, 'btn--link')]")
        print(f"🔗 Se encontraron {len(links)} enlaces.")
        
        hrefs = [link.get_attribute("href") for link in links]
        
        # 5. Procesar cada curso
        for idx, href in enumerate(hrefs, 1):
            print(f"📚 Procesando curso {idx}/{len(hrefs)}: {href}")
            
            try:
                driver.get(href + "&tab=gradebook")
                
                boton_descargas = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'iconDown')]"))
                )
                boton_descargas.click()
                
                botones = WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//button[contains(@class, 'dropdown-item')]"))
                )
                
                time.sleep(2)
                botones[0].click()
                
                driver.get(href + "&tab=gradebook")
                
                boton_actualizar = WebDriverWait(driver, 35).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@title, 'Refresh ')]"))
                )
                boton_actualizar.click()
                
                # Obtener título del curso
                curso = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, "//h1[contains(@class, 'course')]"))
                ).text.strip()
                
                # Sanitizar nombre del curso
                curso = "".join(c for c in curso if c.isalnum() or c in (' ', '-', '_')).strip()
                
                try:
                    abrir_descargaras = WebDriverWait(driver, 30).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'download')]"))
                    )
                    abrir_descargaras.click()
                    
                    primer_link = WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.XPATH, "(//a[contains(@class, 'dropdown-item')])[1]"))
                    )
                    archivos_iniciales = set(os.listdir(download_dir))
                    primer_link.click()
                    
                    # Esperar descarga
                    archivo_descargado = None
                    timeout = 60
                    start_time = time.time()
                    print("Inicio de archivos:", archivos_iniciales)
                    while time.time() - start_time < timeout:
                        archivos_actuales = set(os.listdir(download_dir))
                        print(archivos_actuales)
                        archivos_nuevos = [
                            os.path.join(download_dir, f) 
                            for f in archivos_actuales - archivos_iniciales 
                            if not f.endswith((".part", ".crdownload", ".tmp"))
                        ]
                        
                        if archivos_nuevos:
                            archivo_descargado = max(archivos_nuevos, key=os.path.getctime)
                            
                            # Verificar que descarga completó
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
                        fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
                        extension = os.path.splitext(archivo_descargado)[1]
                        nuevo_nombre = f"{curso}_{fecha}{extension}"
                        ruta_nueva = os.path.join(download_dir, nuevo_nombre)
                        
                        os.rename(archivo_descargado, ruta_nueva)
                        print(f"✅ Archivo guardado: {nuevo_nombre}")
                    else:
                        print(f"❌ No se pudo descargar el archivo para: {curso}")
                        
                except Exception as e:
                    print(f"⚠️ Error al descargar archivo de {curso}: {str(e)}")
                
                time.sleep(3)
                
            except Exception as e:
                print(f"❌ Error procesando curso {href}: {str(e)}")
                continue
        
        print("🎉 Proceso completado exitosamente")
        
    except Exception as e:
        print(f"❌ Error general en el proceso: {str(e)}")
        raise
        
    finally:
        if driver:
            driver.quit()
            print("🔒 Navegador cerrado")

def autenticar_api(**context):
    """
    Función para autenticarse en la API y almacenar el token
    """
    import requests
    
    # Obtener parámetros del contexto
    params = context['dag_run'].conf if context['dag_run'] and context['dag_run'].conf else {}
    
    # Obtener credenciales
    api_url = params.get('api_url', default_params['api_url'])
    username = params.get('api_username', default_params['api_username'])
    password = params.get('api_password', default_params['api_password'])
    
    # Validar parámetros requeridos
    if not password:
        raise ValueError("Se requiere la contraseña de la API")
    
    # Configurar la petición
    login_url = f"{api_url}/api/pg/token/login"
    payload = {"username": username, "password": password}
    headers = {
        "accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        print(f"🔐 Intentando autenticar en {login_url}")
        resp = requests.post(login_url, data=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        
        # Extraer y guardar token
        data = resp.json()
        token = data.get('token') or data.get('access_token')
        if not token:
            raise ValueError(f"No se encontró token en la respuesta: {data}")
            
        context["task_instance"].xcom_push(key="token", value=token)
        print("✅ Token obtenido y almacenado")
        
    except requests.exceptions.RequestException as e:
        print("❌ Error en la autenticación:")
        print(f"   URL: {login_url}")
        print(f"   Usuario: {username}")
        print(f"   Error: {str(e)}")
        if hasattr(e, 'response') and e.response:
            print(f"   Respuesta: {e.response.text}")
        raise

def cargar_archivos_api(**context):
    """
    Función para cargar los archivos descargados usando el token de autenticación
    """
    import requests
    import glob
    
    # Obtener parámetros
    params = context['dag_run'].conf if context['dag_run'] and context['dag_run'].conf else {}
    
    api_url = params.get('api_url', default_params['api_url'])
    download_dir = params.get('download_dir', default_params['download_dir'])
    
    # Obtener token de autenticación
    token = context['task_instance'].xcom_pull(task_ids='autenticar_api', key='token')
    if not token:
        raise ValueError("No se encontró el token de autenticación. La tarea de autenticación falló?")
        
    # Configurar URL y headers
    upload_url = f"{api_url}/api/pg/files/upload"
    headers = {
        'accept': 'application/json',
        'Authorization': token
    }
    
    # Buscar archivos CSV
    archivos = glob.glob(os.path.join(download_dir, "*.csv"))
    print(f"📁 Encontrados {len(archivos)} archivos para cargar")
    
    archivos_cargados = 0
    errores = 0
    
    for archivo in archivos:
        nombre = os.path.basename(archivo)
        print(f"📤 Cargando {nombre}...")
        
        try:
            # Preparar archivo para envío
            with open(archivo, 'rb') as f:
                files = {
                    'file': (nombre, f, 'text/csv'),
                    'assignment': (None, 'string'),
                    'content': (None, 'string')
                }
                
                response = requests.post(upload_url, headers=headers, files=files)
                
                if response.status_code == 200:
                    print(f"✅ Archivo {nombre} cargado exitosamente")
                    archivos_cargados += 1
                    
                    # Eliminar archivo
                    os.remove(archivo)
                    print(f"🗑️ Archivo {nombre} eliminado")
                    
                else:
                    print(f"❌ Error al cargar {nombre}:")
                    print(f"   Código: {response.status_code}")
                    print(f"   Respuesta: {response.text}")
                    errores += 1
                    
        except Exception as e:
            print(f"❌ Error procesando {nombre}: {str(e)}")
            errores += 1
            continue
    
    print("\n📊 Resumen de carga:")
    print(f"   ✅ Archivos cargados: {archivos_cargados}")
    print(f"   ❌ Errores: {errores}")
    
    if errores > 0:
        raise Exception(f"Hubo {errores} errores durante la carga de archivos")

# Definir el DAG
with DAG(
    'netacad_extraccion',
    default_args=default_args,
    description='Descarga de NetAcad y carga a API',
    schedule='0 2 * * *',  # Ejecutar diariamente a las 2 AM
    catchup=False,
    tags=['netacad', 'selenium', 'firefox', 'api'],
    doc_md="""
    # DAG de Extracción de NetAcad y Carga a API
    
    Este DAG descarga datos de cursos desde NetAcad y los carga a una API. Se configura mediante la API de Airflow.
    
    ## Configuración vía API
    
    Para ejecutar el DAG con parámetros personalizados, usa la API de Airflow:
    
    ```bash
    curl -X POST "http://localhost:8080/api/v1/dags/netacad_extraccion/dagRuns" \\
         -H "Content-Type: application/json" \\
         --user "airflow:airflow" \\
         -d '{
           "conf": {
             "email": "usuario@univalle.edu",
             "password": "tu_contraseña",
             "curso_filter": "CCNA",
             "download_dir": "/ruta/personalizada/descargas",
             "api_username": "usuario_api",
             "api_password": "contraseña_api",
             "api_url": "http://localhost:8000"
           }
         }'
    ```
    
    ## Parámetros Disponibles
    
    ### Credenciales NetAcad
    - `email`: Email de NetAcad (requerido)
    - `password`: Contraseña de NetAcad (requerido)
    - `curso_filter`: Filtrar cursos por nombre (opcional)
    - `download_dir`: Directorio para descargas
    
    ### Credenciales API
    - `api_username`: Usuario de la API (requerido)
    - `api_password`: Contraseña de la API (requerido)
    - `api_url`: URL base de la API
    
    ## Flujo de Tareas
    1. Descarga archivos de NetAcad usando Selenium
    2. Autentica en la API y obtiene token
    3. Carga los archivos a la API
    """,
) as dag:
    
    # Tarea de descarga desde NetAcad
    tarea_descargar = PythonOperator(
        task_id='descargar_cursos_netacad',
        python_callable=descargar_cursos_netacad,
    )
    
    # Tarea de autenticación en API
    tarea_auth = PythonOperator(
        task_id='autenticar_api',
        python_callable=autenticar_api,
    )
    
    # Tarea de carga a API
    tarea_cargar = PythonOperator(
        task_id='cargar_archivos_api',
        python_callable=cargar_archivos_api,
    )

    # Definir el orden de ejecución
    tarea_descargar >> tarea_auth >> tarea_cargar