from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
from datetime import datetime

download_dir = os.path.abspath("temp/files")
# Asegurar que el directorio de descargas existe
os.makedirs(download_dir, exist_ok=True)

# Configurar Chrome en modo headless
options = webdriver.ChromeOptions()

# Configurar modo headless con nuevas flags
options.add_argument('--headless=new')  # Nuevo modo headless más estable
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument(f'--window-size=1920,1080')
options.add_argument('--start-maximized')
options.add_argument('--disable-blink-features=AutomationControlled')

# Configurar descargas para modo headless
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

# Deshabilitar la detección de automatización
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(options=options)
link = "https://www.netacad.com/"
email = "asoriag@univalle.edu"
pass_text = "B.e.T.o.2003"
search_content = ""
try:
    # 1. Abrir NetAcad
    driver.get(link)

    # 2. Esperar el botón de login y hacer click
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

    # select_org = WebDriverWait(driver, 20).until(
    #     EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'roleSwitcher')]"))
    # )
    # select_org.click()

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
    # 3. Esperar que cargue la nueva página con los enlaces
    # WebDriverWait(driver, 10).until(
    #     EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@class, 'btn--link')]"))
    # )

    # #organizacion //button[contains(@class, 'roleSwitcher')]

    # 4. Obtener todos los <a> con clase 'btn--link'
    time.sleep(5)
    links = driver.find_elements(By.XPATH, "//a[contains(@class, 'btn--link')]")
    print(f"🔗 Se encontraron {len(links)} enlaces.")
    hrefs = [link.get_attribute("href") for link in links]
    print(hrefs)
    print("🔗 Enlaces encontrados:")
    main_tab = driver.current_window_handle
    for href in hrefs:
        print(href)
        driver.get(href + "&tab=gradebook")
        # complete_link = link + href + "&tab=gradebook"
        # driver.execute_script(f"window.open('{complete_link}', '_blank');")

        # # Cambiar a la nueva pestaña
        # driver.switch_to.window(driver.window_handles[-1])
        # time.sleep(20)
        # # Aquí puedes hacer lo que quieras en la nueva pestaña
        # print("Visitando:", driver.current_url)
        print('llegue aqui')
        boton_descargas = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'iconDown')]"))
        )
        boton_descargas.click()
        print("llegue aqui 1")
        botones = WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.XPATH, "//button[contains(@class, 'dropdown-item')]"))
        )
        print("llegue aqui 2")
        time.sleep(5)
        print(botones)
        # Haz clic solo en el primero (índice 0)
        botones[0].click()
        boton_aceptar = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'btn--primary ')]"))
        )
        driver.get(href + "&tab=gradebook")
        boton_actualizar = WebDriverWait(driver, 35).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(@title, 'Refresh ')]"))
        )
        boton_actualizar.click()

        os.makedirs(download_dir, exist_ok=True)

        # Obtener el título del curso
        curso = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "//h1[contains(@class, 'course')]"))
        ).text.strip()

        try :
            abrir_descargaras = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'download')]"))
            )
            abrir_descargaras.click()
            # Clic al primer enlace de descarga
            primer_link = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, "(//a[contains(@class, 'dropdown-item')])[1]"))
            )
            primer_link.click()

            # Esperar a que se descargue el archivo (revisando la carpeta)
            archivo_descargado = None
            timeout = 60  # segundos
            start_time = time.time()
            archivos_iniciales = set(os.listdir(download_dir))

            while time.time() - start_time < timeout:
                archivos_actuales = set(os.listdir(download_dir))
                archivos_nuevos = [os.path.join(download_dir, f) for f in archivos_actuales - archivos_iniciales 
                                if not f.endswith(".crdownload") and not f.endswith(".tmp")]
                
                if archivos_nuevos:
                    # Tomar el más reciente
                    archivo_descargado = max(archivos_nuevos, key=os.path.getctime)
                    # Esperar a que el archivo termine de descargarse completamente
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

                # Renombrar archivo
                os.rename(archivo_descargado, ruta_nueva)
                print(f"Archivo guardado como: {ruta_nueva}")
            else:
                print("❌ No se pudo detectar el archivo descargado en el tiempo límite.")
        except Exception as e :
            print("Error al intentar descargar el archivo:", e) 
        
        time.sleep(5)  # Esperar 5 segundos para que cargue la página
        # Cerrar la pestaña actual
    driver.close()
finally:
    # Cerrar navegador al terminar
    driver.quit()
