"""Headless Playwright extractor for the Grant Management Portal entities catalog.

Invoked by Airflow via BashOperator inside the unified Airflow container image
where Playwright and Chromium are preinstalled natively. All target URLs,
credentials, routes, and DOM selectors are injected via environment variables
so no tenant-specific configuration is hardcoded in source control.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("extract_portal_catalog")


def _load_config() -> dict[str, object]:
    """Loads extractor settings from environment variables injected by Airflow."""
    portal_url = os.getenv("PORTAL_URL", "").strip().rstrip("/")
    portal_user = os.getenv("PORTAL_USER", "").strip()
    portal_pass = os.getenv("PORTAL_PASS", "").strip()

    if not portal_url:
        raise ValueError("Environment variable PORTAL_URL is required.")
    if not portal_user or not portal_pass:
        raise ValueError("Environment variables PORTAL_USER and PORTAL_PASS are required.")

    return {
        "portal_url": portal_url,
        "portal_user": portal_user,
        "portal_pass": portal_pass,
        "login_path": os.getenv("PORTAL_LOGIN_PATH", "/users/sign_in").strip(),
        "report_path": os.getenv("PORTAL_REPORT_PATH", "/entities/catalog").strip(),
        "output_path": os.getenv("OUTPUT_PATH", "/opt/airflow/data/portal_entities_catalog.xlsx"),
        "error_dir": os.getenv("ERROR_ARTIFACT_DIR", "/opt/airflow/data/errors"),
        "export_timeout_ms": int(os.getenv("EXPORT_TIMEOUT_MS", "120000")),
        "dom_attach_timeout_ms": int(os.getenv("DOM_ATTACH_TIMEOUT_MS", "15000")),
        "headless": os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true",
        "email_selector": os.getenv("LOGIN_EMAIL_SELECTOR", "#user_email"),
        "password_selector": os.getenv("LOGIN_PASSWORD_SELECTOR", "#user_password"),
        "submit_selector": os.getenv("LOGIN_SUBMIT_SELECTOR", "input[type=submit]"),
        "menu_selector": os.getenv("MENU_TRIGGER_SELECTOR", "a.shared-new-button"),
        "export_selector": os.getenv(
            "EXPORT_TRIGGER_SELECTOR",
            "a.graph-links[href*='generate_excel'], a.graph-links[href*='generate_students_excel']",
        ),
        "download_selector": os.getenv("DOWNLOAD_LINK_SELECTOR", "text=Descargar archivo"),
    }


def _save_error_artifacts(page, error_dir: str, prefix: str = "portal_error") -> None:
    """Persists screenshot and DOM snapshot for post-mortem debugging."""
    target_dir = Path(error_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    screenshot_path = target_dir / f"{prefix}_{stamp}.png"
    html_path = target_dir / f"{prefix}_{stamp}.html"

    page.screenshot(path=str(screenshot_path), full_page=True)
    html_path.write_text(page.content(), encoding="utf-8")
    logger.error("Saved diagnostic artifacts to %s and %s", screenshot_path, html_path)


def extract_portal_catalog() -> Path:
    """Authenticates into the grant portal, triggers async Excel generation, and saves output."""
    cfg = _load_config()
    output_path = Path(str(cfg["output_path"]))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    login_url = f"{cfg['portal_url']}/{str(cfg['login_path']).lstrip('/')}"
    report_url = f"{cfg['portal_url']}/{str(cfg['report_path']).lstrip('/')}"

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool(cfg["headless"]))
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            logger.info("Navigating to portal login page: %s", login_url)
            page.goto(login_url, wait_until="domcontentloaded")
            page.fill(str(cfg["email_selector"]), str(cfg["portal_user"]))
            page.fill(str(cfg["password_selector"]), str(cfg["portal_pass"]))

            with page.expect_navigation():
                page.click(str(cfg["submit_selector"]))

            if "sign_in" in page.url.lower():
                _save_error_artifacts(page, str(cfg["error_dir"]), prefix="login_failure")
                raise RuntimeError("Portal authentication failed: redirected back to login page.")

            logger.info("Authentication succeeded. Navigating to catalog route: %s", report_url)
            page.goto(report_url, wait_until="networkidle")

            logger.info("Triggering asynchronous server-side Excel export...")
            page.click(str(cfg["menu_selector"]))

            # Wait for async dropdown node attachment in the DOM, then invoke native JS click
            # to avoid animation visibility race conditions in headless Chromium.
            export_locator = page.locator(str(cfg["export_selector"])).first
            export_locator.wait_for(state="attached", timeout=int(cfg["dom_attach_timeout_ms"]))
            export_locator.evaluate("node => node.click()")

            logger.info(
                "Waiting up to %d ms for server report generation...",
                int(cfg["export_timeout_ms"]),
            )
            page.wait_for_selector(
                str(cfg["download_selector"]),
                timeout=int(cfg["export_timeout_ms"]),
            )

            with page.expect_download() as download_info:
                page.click(str(cfg["download_selector"]))

            download = download_info.value
            download.save_as(str(output_path))
            logger.info("Portal catalog saved successfully to %s", output_path)
            return output_path

        except Exception:
            try:
                _save_error_artifacts(page, str(cfg["error_dir"]), prefix="extract_failure")
            except Exception as capture_err:
                logger.warning("Could not save error diagnostic artifacts: %s", capture_err)
            raise
        finally:
            browser.close()


if __name__ == "__main__":
    try:
        extract_portal_catalog()
    except Exception as exc:
        logger.error("Critical portal extraction failure: %s", exc)
        sys.exit(1)
