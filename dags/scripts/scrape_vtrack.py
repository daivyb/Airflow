"""
scrape_vtrack.py
================
Script de scraping para vtrack.cl usando Playwright (headless).
Se ejecuta DENTRO del contenedor mcr.microsoft.com/playwright/python
lanzado por el DockerOperator de Airflow.

El reporte descargado se guarda en /data/reporte_completo.xlsx,
ruta que está mapeada al volumen compartido ./data del host.
"""

import os
import sys
from playwright.sync_api import sync_playwright

# =========================================================
# CONFIGURACIÓN
# =========================================================

# Variables de entorno inyectadas por Airflow
VTRACK_URL = os.getenv("VTRACK_URL", "https://profonanpe.vtrack.cl")
VTRACK_USER = os.getenv("VTRACK_USER", "")
VTRACK_PASS = os.getenv("VTRACK_PASS", "")

# Por defecto, en Docker esto escribe en el volumen mapeado.
# Si lo ejecutas localmente en tu PC para probar, lo guardará en tu C:\ (o donde estés).
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/data/reporte_completo.xlsx")
TIMEOUT_GENERACION_MS = 120000  # 2 minutos


def scrape_reporte():
    """
    Navega a vtrack.cl, inicia sesión, descarga el reporte
    completo de entidades y lo guarda en OUTPUT_PATH.
    """
    if not VTRACK_USER or not VTRACK_PASS:
        raise ValueError("Credenciales VTRACK_USER o VTRACK_PASS no configuradas.")

    # Si se corre fuera de Docker, se puede poner headless=False para debugear
    # pero en producción (Airflow) debe ser True.
    is_headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=is_headless)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            # ----- 1. Login -----
            login_url = f"{VTRACK_URL.rstrip('/')}/users/sign_in"
            print(f"[scrape_vtrack] Navegando a {login_url}")
            page.goto(login_url)
            
            page.fill("#user_email", VTRACK_USER)
            page.fill("#user_password", VTRACK_PASS)
            
            with page.expect_navigation():
                page.click("input[type=submit]")
                
            print(f"[scrape_vtrack] Login completado. URL actual: {page.url}")
            
            # Verificar si nos redirigió de vuelta al login o si hay un mensaje de error
            if "sign_in" in page.url:
                print("[scrape_vtrack] ERROR: Seguimos en la página de login. Imprimiendo contenido...")
                with open("login_error.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                raise ValueError("El login falló (credenciales inválidas o captcha).")

            # ----- 2. Ir a la tabla de proyectos -----
            tabla_url = f"{VTRACK_URL.rstrip('/')}/student_types/382/students"
            print(f"[scrape_vtrack] Navegando a tabla de proyectos: {tabla_url}")
            page.goto(tabla_url)
            page.wait_for_load_state("networkidle")

            # ----- 3. Abrir el menú "Más" -----
            print("[scrape_vtrack] Abriendo opciones de descarga...")
            page.click("a.shared-new-button")

            # Esperamos que el elemento exista en el DOM
            btn_locator = page.locator("a.graph-links[href*='generate_students_excel']")
            btn_locator.wait_for(state="attached", timeout=15000)
            # Forzamos el clic vía Javascript asegurando que el nodo ya existe
            btn_locator.evaluate("node => node.click()")

            # ----- 5. Esperar a que aparezca el botón "Descargar archivo" -----
            print("[scrape_vtrack] Esperando generación del reporte en el servidor (hasta 2 min)...")
            page.wait_for_selector("text=Descargar archivo", timeout=TIMEOUT_GENERACION_MS)

            # ----- 6. Click y capturar la descarga real -----
            print("[scrape_vtrack] Reporte generado. Iniciando descarga...")
            with page.expect_download() as download_info:
                page.click("text=Descargar archivo")
            
            download = download_info.value
            
            # Asegurarse que la carpeta de destino exista (útil si se prueba localmente)
            os.makedirs(os.path.dirname(os.path.abspath(OUTPUT_PATH)), exist_ok=True)
            
            download.save_as(OUTPUT_PATH)
            print(f"[scrape_vtrack] [OK] Reporte guardado exitosamente en: {OUTPUT_PATH}")

        except Exception as e:
            print(f"[scrape_vtrack] Capturando screenshot por error...")
            # Save to /opt/airflow/data/ so it persists and syncs to Windows host
            page.screenshot(path="/opt/airflow/data/error_screenshot.png")
            with open("/opt/airflow/data/error_page.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            raise e
        finally:
            browser.close()


if __name__ == "__main__":
    try:
        scrape_reporte()
    except Exception as e:
        print(f"[scrape_vtrack] ERROR CRÍTICO: {e}", file=sys.stderr)
        sys.exit(1)
