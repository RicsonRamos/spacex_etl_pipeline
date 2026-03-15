#!/usr/bin/env python3
"""
Script de bootstrap do Metabase - Configura admin e conexão com banco automaticamente
"""
import requests
import time
import os
import sys

METABASE_URL = os.getenv("MB_SETUP_URL", "http://metabase:3000")
ADMIN_EMAIL = os.getenv("MB_ADMIN_EMAIL", "admin@spacex.local")
ADMIN_PASSWORD = os.getenv("MB_ADMIN_PASSWORD", "SpaceX2026!")
ADMIN_FIRST_NAME = os.getenv("MB_ADMIN_FIRST_NAME", "SpaceX")
ADMIN_LAST_NAME = os.getenv("MB_ADMIN_LAST_NAME", "Admin")

# Configuração do banco SpaceX
DB_HOST = os.getenv("POSTGRES_HOST", "spacex_postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "spacex_db")
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

def wait_for_metabase(timeout=300):
    """Aguarda o Metabase ficar pronto"""
    print(" Aguardando Metabase iniciar...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                print(" Metabase está pronto!")
                return True
        except:
            pass
        time.sleep(2)
    print(" Timeout aguardando Metabase")
    return False

def setup_admin():
    """Cria usuário admin via API de setup"""
    print(f"🔧 Criando admin: {ADMIN_EMAIL}")
    
    # Verifica se já existe setup
    try:
        resp = requests.get(f"{METABASE_URL}/api/session/properties", timeout=10)
        props = resp.json()
        
        # Se já tem setup-token, significa que ainda não foi configurado
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
            
            resp = requests.post(f"{METABASE_URL}/api/setup", json=payload, timeout=30)
            if resp.status_code == 200:
                print(f" Admin criado: {ADMIN_EMAIL}")
                return resp.json().get("id")
            else:
                print(f" Erro no setup: {resp.text}")
                return None
        else:
            print(" Setup já realizado anteriormente")
            return True
            
    except Exception as e:
        print(f" Erro: {e}")
        return None

def login_and_get_token():
    """Login e retorna token de sessão"""
    try:
        resp = requests.post(f"{METABASE_URL}/api/session", json={
            "username": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        }, timeout=10)
        
        if resp.status_code == 200:
            return resp.json().get("id")
    except Exception as e:
        print(f" Erro no login: {e}")
    return None

def setup_database_connection(session_token):
    """Configura conexão com banco SpaceX"""
    print(f"🔌 Configurando conexão com banco: {DB_NAME}")
    
    headers = {"X-Metabase-Session": session_token}
    
    # Verifica se conexão já existe
    try:
        resp = requests.get(f"{METABASE_URL}/api/database", headers=headers, timeout=10)
        databases = resp.json().get("data", [])
        
        for db in databases:
            if db.get("name") == "SpaceX Production":
                print(" Conexão 'SpaceX Production' já existe")
                return True
    except:
        pass
    
    payload = {
        "engine": "postgres",
        "name": "SpaceX Production",
        "details": {
            "host": DB_HOST,
            "port": int(DB_PORT),
            "dbname": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD,
            "ssl": False,
            "tunnel-enabled": False
        },
        "auto_run_queries": True,
        "is_full_sync": True,
        "is_on_demand": False
    }
    
    try:
        resp = requests.post(f"{METABASE_URL}/api/database", 
                           headers=headers, 
                           json=payload, 
                           timeout=30)
        
        if resp.status_code == 200:
            print(f" Conexão 'SpaceX Production' criada!")
            return True
        else:
            print(f" Erro ao criar conexão: {resp.text}")
            return False
    except Exception as e:
        print(f" Erro: {e}")
        return False

def main():
    print(" Iniciando setup automático do Metabase...")
    
    if not wait_for_metabase():
        sys.exit(1)
    
    # Setup admin
    admin_ok = setup_admin()
    if not admin_ok:
        print(" Falha no setup do admin, tentando continuar...")
    
    # Aguarda um pouco após setup
    time.sleep(5)
    
    # Login e configuração do banco
    session_token = login_and_get_token()
    if not session_token:
        print(" Não foi possível fazer login")
        sys.exit(1)
    
    print("Login realizado com sucesso")
    
    # Configura banco
    if setup_database_connection(session_token):
        print("\nMetabase totalmente configurado!")
        print(f"Acesse: http://localhost:3000")
        print(f"Email: {ADMIN_EMAIL}")
        print(f"Senha: {ADMIN_PASSWORD}")
        print(f"Banco: SpaceX Production")
        return 0
    else:
        print("Configuração do banco falhou")
        return 1

if __name__ == "__main__":
    sys.exit(main())