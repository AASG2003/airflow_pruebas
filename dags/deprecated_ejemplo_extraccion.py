"""
DAG para la extracción automática de gradebooks de NetAcad - Versión todo en uno
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
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

def setup_firefox_driver():
    """Configurar el driver de Firefox y directorios necesarios"""
    context = get_current_context()
    
    # Configurar directorios
    airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    download_dir = os.path.join(airflow_home, 'netacad_downloads')
    os.makedirs(download_dir, exist_ok=True)
    
    print(f"🏠 AIRFLOW_HOME: {airflow_home}")
    print(f"📂 Download directory creado: {download_dir}")
    print(f"✅ Directory exists: {os.path.exists(download_dir)}")
    
    # Configurar Firefox en modo headless para Docker (más ligero que Chrome)
    options = webdriver.FirefoxOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    
    # Configurar perfil de Firefox para descargas automáticas
    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.download.folderList", 2)
    profile.set_preference("browser.download.manager.showWhenStarting", False)
    profile.set_preference("browser.download.dir", download_dir)
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk", 
                          "application/vnd.ms-excel,application/csv,text/csv,application/octet-stream")
    profile.set_preference("browser.helperApps.alwaysAsk.force", False)
    profile.set_preference("browser.download.manager.alertOnEXEOpen", False)
    profile.set_preference("browser.download.manager.focusWhenStarting", False)
    profile.set_preference("browser.download.manager.useWindow", False)
    profile.set_preference("browser.download.manager.showAlertOnComplete", False)
    profile.set_preference("browser.download.manager.closeWhenDone", False)
    
    # Guardar configuración en el contexto
    try:
        context['task_instance'].xcom_push(key='download_dir', value=download_dir)
        print(f"✅ download_dir guardado en XCom: {download_dir}")
    except Exception as e:
        print(f"❌ Error guardando download_dir en XCom: {e}")
    
    # Serializar las opciones como lista simple de strings
    try:
        option_strings = [
            '--headless',
            '--no-sandbox', 
            '--disable-dev-shm-usage',
            '--window-size=1920,1080'
        ]
        context['task_instance'].xcom_push(key='firefox_options', value=option_strings)
        print(f"✅ Opciones de Firefox guardadas: {option_strings}")
    except Exception as e:
        print(f"⚠️ Error al guardar opciones: {e}")
    
    # Guardar solo el directorio base del perfil
    try:
        context['task_instance'].xcom_push(key='firefox_profile_path', value=profile.path)
        print(f"✅ Perfil de Firefox guardado en: {profile.path}")
    except Exception as e:
        print(f"⚠️ Error al guardar perfil: {e}")
        context['task_instance'].xcom_push(key='firefox_profile_path', value=None)
    
    return "Configuración de Firefox completada"

def extract_gradebooks():
    """Ejecutar la extracción de gradebooks"""
    context = get_current_context()
    
    download_dir = context['task_instance'].xcom_pull(key='download_dir', task_ids='setup_firefox_driver')
    firefox_options_list = context['task_instance'].xcom_pull(key='firefox_options', task_ids='setup_firefox_driver')
    firefox_profile_path = context['task_instance'].xcom_pull(key='firefox_profile_path', task_ids='setup_firefox_driver')
    
    print(f"🔍 XCom data received:")
    print(f"   - download_dir: {download_dir}")
    print(f"   - firefox_options: {firefox_options_list}")
    print(f"   - profile_path: {firefox_profile_path}")
    
    # Si download_dir es None, recrearlo aquí
    if not download_dir:
        airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
        download_dir = os.path.join(airflow_home, 'netacad_downloads')
        os.makedirs(download_dir, exist_ok=True)
        print(f"⚠️ Recreando download_dir porque llegó como None: {download_dir}")
    
    print(f"📂 Directorio de descargas configurado: {download_dir}")
    print(f"📁 ¿Existe el directorio?: {os.path.exists(download_dir) if download_dir else 'No definido'}")
    if download_dir and os.path.exists(download_dir):
        print(f"📄 Archivos existentes: {os.listdir(download_dir)}")
    
    # Recrear las opciones de Firefox con manejo de errores
    options = webdriver.FirefoxOptions()
    
    # Configurar opciones básicas siempre (por si XCom falla)
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    
    # Agregar opciones adicionales desde XCom si están disponibles
    if firefox_options_list and isinstance(firefox_options_list, list):
        for opt in firefox_options_list:
            if opt not in options.arguments:  # Evitar duplicados
                options.add_argument(opt)
    else:
        print("⚠️ No se pudieron recuperar las opciones de Firefox desde XCom, usando opciones por defecto")
    
    # Recrear el perfil de Firefox con manejo de errores
    profile = None
    if firefox_profile_path and os.path.exists(firefox_profile_path):
        try:
            profile = webdriver.FirefoxProfile(firefox_profile_path)
        except Exception as e:
            print(f"⚠️ Error al cargar perfil desde {firefox_profile_path}: {e}")
            profile = None
    
    if not profile:
        # Crear un perfil básico si no se puede recuperar desde XCom
        profile = webdriver.FirefoxProfile()
        if download_dir:
            profile.set_preference("browser.download.folderList", 2)
            profile.set_preference("browser.download.manager.showWhenStarting", False)
            profile.set_preference("browser.download.dir", download_dir)
            profile.set_preference("browser.helperApps.neverAsk.saveToDisk", 
                                  "application/vnd.ms-excel,application/csv,text/csv,application/octet-stream")
            profile.set_preference("browser.helperApps.alwaysAsk.force", False)
    
    # Credenciales (usar Variables de Airflow en producción)
    email = "asoriag@univalle.edu"
    pass_text = "B.e.T.o.2003"
    
    # Configurar el perfil en las opciones (método moderno de Selenium)
    if profile:
        options.profile = profile
    
    driver = None
    try:
        # Usar webdriver-manager para Docker con Firefox
        service = Service(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=options)
        
        print("🌐 Iniciando navegación a NetAcad...")
        
        # 1. Abrir NetAcad
        driver.get("https://www.netacad.com/")
        print("✅ Página de NetAcad cargada")

        # 2. Login
        print("🔍 Buscando botón de login...")
        login_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'login')]"))
        )
        login_button.click()
        print("🖱️ Botón de login clickeado")

        print("🔍 Ingresando email...")
        username = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//input[@name='username']"))
        )
        username.send_keys(email)
        print(f"✅ Email ingresado: {email}")

        print("🔍 Buscando botón de continuar después del email...")
        login_button = driver.find_element(By.XPATH, "//input[contains(@name, 'login')]")
        login_button.click()
        print("🖱️ Botón de continuar clickeado")

        print("🔍 Ingresando contraseña...")
        password = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//input[@name='password']"))
        )
        password.send_keys(pass_text)
        print("✅ Contraseña ingresada")
        
        login_button = driver.find_element(By.XPATH, "//input[contains(@name, 'login')]")
        login_button.click()
        print("🖱️ Login completado")

        # 3. Buscar cursos
        print("🔍 Buscando barra de búsqueda...")
        searcher = WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((By.XPATH, "//input[@id='searchContent']"))
        )
        searcher.send_keys("")
        print("✅ Campo de búsqueda encontrado")

        print("🔍 Ejecutando búsqueda...")
        search = WebDriverWait(driver, 25).until(
            EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='search']"))
        )
        search.click()
        print("🖱️ Búsqueda ejecutada")
        
        print("🔍 Configurando para mostrar 100 resultados...")
        show_all = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//option[text()='100']"))
        )
        show_all.click()
        print("✅ Configurado para mostrar 100 resultados")

        print("⏳ Esperando carga de resultados...")
        time.sleep(5)
        
        print("🔍 Obteniendo enlaces de cursos...")
        links = driver.find_elements(By.XPATH, "//a[contains(@class, 'btn--link')]")
        print(f"🔗 Se encontraron {len(links)} enlaces.")
        hrefs = [link.get_attribute("href") for link in links]
        print(f"📋 URLs obtenidas: {len(hrefs)}")

        # 4. Procesar cada curso con flujo completo
        for i, href in enumerate(hrefs, 1):
            try:
                print(f"\n🔄 Procesando curso {i}/{len(hrefs)}: {href}")
                gradebook_url = href + "&tab=gradebook"
                print(f"🌐 Navegando a: {gradebook_url}")
                driver.get(gradebook_url)
                
                # PASO 1: Primer intento - Descargar gradebook con botón iconDown
                print("🔍 MÉTODO PRINCIPAL: Buscando botón de descargas (iconDown)...")
                primera_descarga_exitosa = False
                try:
                    boton_descargas = WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'iconDown')]"))
                    )
                    print("✅ Botón de descargas encontrado")
                    driver.execute_script("arguments[0].click();", boton_descargas)
                    print("🖱️ Botón de descargas clickeado (JavaScript)")

                    print("🔍 Buscando opciones del dropdown...")
                    botones = WebDriverWait(driver, 20).until(
                        EC.presence_of_all_elements_located((By.XPATH, "//button[contains(@class, 'dropdown-item')]"))
                    )
                    print(f"✅ {len(botones)} opciones de descarga encontradas")
                    
                    if len(botones) == 0:
                        print("❌ No se encontraron opciones de descarga")
                        raise Exception("No dropdown items found")
                    
                    # Usar JavaScript click para mayor confiabilidad
                    driver.execute_script("arguments[0].click();", botones[0])
                    print("🖱️ Primera opción de descarga clickeada (JavaScript)")

                    # Esperar y aceptar la descarga
                    print("🔍 Buscando botón de confirmación...")
                    boton_aceptar = WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn--primary ')]"))
                    )
                    print("✅ Botón de confirmación encontrado")
                    driver.execute_script("arguments[0].click();", boton_aceptar)
                    print("🖱️ Botón de confirmación clickeado - iniciando descarga...")
                    
                    primera_descarga_exitosa = True
                    
                except Exception as e:
                    print(f"⚠️ Método principal falló: {e}")
                    primera_descarga_exitosa = False

                # MÉTODO ALTERNATIVO: Si el primer método falla
                if not primera_descarga_exitosa:
                    print("\n🔄 MÉTODO ALTERNATIVO: Recargando página...")
                    try:
                        # Recargar la página del gradebook
                        driver.get(gradebook_url)
                        time.sleep(3)
                        
                        # Buscar botón de refresh/actualizar
                        print("🔍 Buscando botón de actualizar...")
                        boton_actualizar = WebDriverWait(driver, 35).until(
                            EC.element_to_be_clickable((By.XPATH, "//button[contains(@title, 'Refresh ')]"))
                        )
                        driver.execute_script("arguments[0].click();", boton_actualizar)
                        print("🖱️ Botón de actualizar clickeado")
                        
                        time.sleep(5)  # Esperar a que se actualice
                        
                        # Buscar botón de descarga alternativo
                        print("🔍 Buscando botón de descarga alternativo...")
                        abrir_descargas = WebDriverWait(driver, 30).until(
                            EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'download')]"))
                        )
                        driver.execute_script("arguments[0].click();", abrir_descargas)
                        print("🖱️ Botón de descarga alternativo clickeado")
                        
                        # Buscar enlace de descarga (no botón)
                        print("🔍 Buscando enlace de descarga...")
                        primer_link = WebDriverWait(driver, 25).until(
                            EC.element_to_be_clickable((By.XPATH, "(//a[contains(@class, 'dropdown-item')])[1]"))
                        )
                        driver.execute_script("arguments[0].click();", primer_link)
                        print("🖱️ Enlace de descarga clickeado")
                        
                    except Exception as e2:
                        print(f"❌ Método alternativo también falló: {e2}")
                        print("⏭️ Saltando al siguiente curso...")
                        continue

                # Obtener título del curso con múltiples selectores
                curso = f"Curso_{i}_Sin_Titulo"
                try:
                    # Intentar diferentes selectores para el título
                    selectores_titulo = [
                        "//h1[contains(@class, 'course')]",
                        "//h1[contains(@class, 'title')]", 
                        "//h1",
                        "//span[contains(@class, 'course-title')]"
                    ]
                    
                    for selector in selectores_titulo:
                        try:
                            elemento_titulo = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, selector))
                            )
                            curso_temp = elemento_titulo.text.strip()
                            if curso_temp and len(curso_temp) > 3:  # Validar que no esté vacío
                                curso = curso_temp
                                print(f"📚 Título del curso obtenido: {curso}")
                                break
                        except:
                            continue
                    
                    if curso.startswith("Curso_"):
                        print(f"⚠️ No se pudo obtener título del curso, usando: {curso}")
                        
                except Exception as e:
                    print(f"⚠️ Error obteniendo título: {e}")

                # Esperar y verificar descarga con logging detallado
                archivo_descargado = None
                try:
                    archivos_iniciales = set(os.listdir(download_dir))
                    print(f"📋 Archivos iniciales en {download_dir}: {archivos_iniciales}")
                except Exception as e:
                    print(f"❌ Error listando directorio inicial: {e}")
                    archivos_iniciales = set()
                    
                start_time = time.time()
                timeout_descarga = 90  # 90 segundos timeout para Firefox

                print(f"⏳ Iniciando monitoreo de descarga (timeout: {timeout_descarga}s)...")
                while time.time() - start_time < timeout_descarga:
                    try:
                        archivos_actuales = set(os.listdir(download_dir))
                        archivos_nuevos_completos = []
                        
                        for archivo in archivos_actuales - archivos_iniciales:
                            # Filtrar archivos temporales específicos de Firefox
                            if not archivo.endswith((".crdownload", ".tmp", ".part", ".download")):
                                ruta_archivo = os.path.join(download_dir, archivo)
                                if os.path.exists(ruta_archivo):
                                    archivos_nuevos_completos.append(ruta_archivo)
                        
                        # Logging de progreso cada 10 segundos
                        elapsed = time.time() - start_time
                        if int(elapsed) % 10 == 0 and elapsed > 0:
                            archivos_temporales = [f for f in archivos_actuales - archivos_iniciales 
                                                 if f.endswith((".crdownload", ".tmp", ".part", ".download"))]
                            print(f"⏳ Monitoreo descarga... {elapsed:.0f}s/{timeout_descarga}s")
                            if archivos_temporales:
                                print(f"🔄 Archivos en descarga: {archivos_temporales}")
                        
                        if archivos_nuevos_completos:
                            archivo_descargado = max(archivos_nuevos_completos, key=os.path.getctime)
                            print(f"📥 Archivo detectado: {archivo_descargado}")
                            
                            # Verificar estabilidad del archivo (descarga completa)
                            tamaño_anterior = -1
                            verificaciones = 0
                            while verificaciones < 15:  # Máximo 15 verificaciones
                                try:
                                    tamaño_actual = os.path.getsize(archivo_descargado)
                                    if verificaciones % 3 == 0:  # Log cada 3 verificaciones
                                        print(f"📊 Verificación {verificaciones + 1}: {tamaño_actual} bytes")
                                    
                                    if tamaño_actual == tamaño_anterior and tamaño_actual > 0:
                                        print(f"✅ Descarga estable: {tamaño_actual} bytes")
                                        time.sleep(1)  # Espera adicional de seguridad
                                        break
                                    tamaño_anterior = tamaño_actual
                                    verificaciones += 1
                                    time.sleep(1)
                                except OSError as e:
                                    print(f"⚠️ Error accediendo al archivo (intento {verificaciones}): {e}")
                                    time.sleep(1)
                                    continue
                            break
                    except Exception as e:
                        print(f"❌ Error durante monitoreo: {e}")
                # Procesar resultado de la descarga
                    # Renombrar archivo
                    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
                    extension = os.path.splitext(archivo_descargado)[1]
                    nuevo_nombre = f"{curso}_{fecha}{extension}"
                    ruta_nueva = os.path.join(download_dir, nuevo_nombre)
                    os.rename(archivo_descargado, ruta_nueva)
                    print(f"✅ Archivo guardado: {nuevo_nombre}")
                    print(f"📁 Ruta completa: {ruta_nueva}")
                    print(f"📊 Tamaño: {os.path.getsize(ruta_nueva)} bytes")
                else:
                    # Diagnóstico detallado del fallo
                    archivos_finales = set(os.listdir(download_dir))
                    tiempo_transcurrido = time.time() - start_time
                    
                    print(f"❌ FALLO EN DESCARGA - Curso: {curso}")
                    print(f"⏰ Tiempo transcurrido: {tiempo_transcurrido:.1f}s (límite: 60s)")
                    print(f"📂 Directorio: {download_dir}")
                    print(f"📋 Archivos iniciales: {archivos_iniciales}")
                    print(f"📋 Archivos finales: {archivos_finales}")
                    print(f"🔄 Archivos que aparecieron: {archivos_finales - archivos_iniciales}")
                    
                    # Verificar archivos temporales que pueden haber quedado
                    archivos_temporales = [f for f in archivos_finales 
                                         if f.endswith((".crdownload", ".tmp", ".part"))]
                    if archivos_temporales:
                        print(f"⚠️ Archivos temporales encontrados: {archivos_temporales}")
                        print("💡 Posible causa: Descarga interrumpida o muy lenta")
                    
                    # Verificar permisos del directorio
                    try:
                        test_file = os.path.join(download_dir, "test_permisos.txt")
                        with open(test_file, 'w') as f:
                            f.write("test")
                        os.remove(test_file)
                        print("✅ Permisos de escritura: OK")
                    except Exception as perm_error:
                        print(f"❌ Error de permisos: {perm_error}")
                    
                    # Posibles causas
                    if tiempo_transcurrido >= 60:
                        print("� Posible causa: TIMEOUT - El archivo tardó más de 60s en descargarse")
                    elif not archivos_finales - archivos_iniciales:
                        print("🔍 Posible causa: NO SE INICIÓ LA DESCARGA")
                        print("   - Verificar si el botón de descarga funciona")
                        print("   - Revisar configuración del perfil de Firefox")
                        print("   - Verificar si hay pop-ups o confirmaciones bloqueando")
                    else:
                        print("🔍 Posible causa: ARCHIVO TEMPORAL NO COMPLETADO")
                        print("   - Conexión lenta o interrumpida")
                        print("   - Archivo muy grande para el timeout actual")
                
                time.sleep(5)
            except Exception as e:
                print(f"❌ Error procesando curso {href}: {str(e)}")
                continue

        return "Extracción completada"
    
    except Exception as e:
        error_msg = f"Error en la extracción: {str(e)}"
        print(error_msg)
        context['task_instance'].xcom_push(key='error', value=error_msg)
        raise
    
    finally:
        if driver:
            driver.quit()

# Definición del DAG
with DAG(
    'netacad_gradebook_extraction_simple',
    default_args=default_args,
    description='Extrae gradebooks de Cisco NetAcad - Versión simplificada',
    schedule=None,  # Cambio de schedule_interval a schedule para Airflow 3
    start_date=datetime(2025, 1, 1),  # Cambio de days_ago a datetime
    catchup=False,
    tags=['netacad', 'gradebooks'],
) as dag:
    
    # Tarea de configuración - Cambio a Firefox
    setup_task = PythonOperator(
        task_id='setup_firefox_driver',
        python_callable=setup_firefox_driver,
    )
    
    # Tarea de extracción - Quitar provide_context (deprecado en Airflow 3)
    extract_task = PythonOperator(
        task_id='extract_gradebooks',
        python_callable=extract_gradebooks,
    )
    
    # Definir el orden de las tareas
    setup_task >> extract_task