#!/usr/bin/env python3
"""
Metabase bootstrap script
Configura automaticamente admin e conexão com banco
"""

import os
import sys
import time
import requests

from dotenv import load_dotenv
from utils.logger import get_logger

# ------------------------------
# ENV
# ------------------------------

load_dotenv()

logger = get_logger("MainOrchestrator")


def get_env(name: str):
    """Busca variável obrigatória"""
    value = os.getenv(name)

    if not value:
        logger.error(f"Environment variable not set: {name}")
        sys.exit(1)

    return value


# ------------------------------
# METABASE CONFIG
# ------------------------------

METABASE_URL = get_env("MB_SETUP_URL")

ADMIN_EMAIL = get_env("MB_ADMIN_EMAIL")
ADMIN_PASSWORD = get_env("MB_ADMIN_PASSWORD")
ADMIN_FIRST_NAME = get_env("MB_ADMIN_FIRST_NAME")
ADMIN_LAST_NAME = get_env("MB_ADMIN_LAST_NAME")

# ------------------------------
# DATABASE CONFIG
# ------------------------------

DB_HOST = get_env("POSTGRES_HOST")
DB_PORT = get_env("POSTGRES_PORT")
DB_NAME = get_env("POSTGRES_DB")
DB_USER = get_env("POSTGRES_USER")
DB_PASSWORD = get_env("POSTGRES_PASSWORD")


def wait_for_metabase(timeout=300):
    """Aguarda Metabase iniciar"""

    logger.info("Waiting for Metabase to start...")

    start = time.time()

    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{METABASE_URL}/api/health", timeout=5)

            if resp.status_code == 200 and resp.json().get("status") == "ok":
                logger.info("Metabase is ready")
                return True

        except Exception:
            pass

        time.sleep(2)

    logger.error("Timeout waiting for Metabase")
    return False


def setup_admin():
    """Cria admin via API"""

    logger.info("Creating admin user")

    try:
        resp = requests.get(f"{METABASE_URL}/api/session/properties", timeout=10)
        props = resp.json()

        if "setup-token" in props and props["setup-token"]:

            token = props["setup-token"]

            payload = {
                "token": token,
                "user": {
                    "email": ADMIN_EMAIL,
                    "first_name": ADMIN_FIRST_NAME,
                    "last_name": ADMIN_LAST_NAME,
                    "password": ADMIN_PASSWORD
                },
                "prefs": {
                    "site_name": "SpaceX Analytics",
                    "site_locale": "pt-BR",
                    "allow_tracking": False
                }
            }

            resp = requests.post(
                f"{METABASE_URL}/api/setup",
                json=payload,
                timeout=30
            )

            if resp.status_code == 200:
                logger.info("Admin created successfully")
                return True

            logger.error(f"Metabase setup error: {resp.text}")

        else:
            logger.info("Metabase already configured")
            return True

    except Exception as e:
        logger.error(f"Error during admin setup: {e}")

    return False


def login_and_get_token():
    """Login no Metabase"""

    try:
        resp = requests.post(
            f"{METABASE_URL}/api/session",
            json={
                "username": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            },
            timeout=10
        )

        if resp.status_code == 200:
            logger.info("Login successful")
            return resp.json()["id"]

    except Exception as e:
        logger.error(f"Login error: {e}")

    return None


def setup_database_connection(session_token):
    """Configura conexão postgres"""

    logger.info("Configuring database connection")

    headers = {"X-Metabase-Session": session_token}

    payload = {
        "engine": "postgres",
        "name": "SpaceX Production",
        "details": {
            "host": DB_HOST,
            "port": int(DB_PORT),
            "dbname": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD,
            "ssl": False
        }
    }

    try:
        resp = requests.post(
            f"{METABASE_URL}/api/database",
            headers=headers,
            json=payload,
            timeout=30
        )

        if resp.status_code == 200:
            logger.info("Database connection created")
            return True

        logger.error(f"Database creation failed: {resp.text}")

    except Exception as e:
        logger.error(f"Database setup error: {e}")

    return False


def main():

    logger.info("Starting Metabase bootstrap process")

    if not wait_for_metabase():
        sys.exit(1)

    setup_admin()

    time.sleep(5)

    token = login_and_get_token()

    if not token:
        logger.error("Login failed")
        sys.exit(1)

    if setup_database_connection(token):
        logger.info("Metabase fully configured")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()