import psycopg2
import os
from contextlib import contextmanager
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env automaticamente
load_dotenv()

class DatabaseConfig:
    """
    Configuração segura do banco usando variáveis de ambiente.
    As credenciais ficam no arquivo .env (não versionado).
    """
    
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'eSaudeCuritiba'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
        'port': os.getenv('DB_PORT', '5432')
    }
    
    @classmethod
    def test_connection(cls):
        """Testa se as variáveis de ambiente estão configuradas"""
        missing = []
        for key, value in cls.DB_CONFIG.items():
            if not value and key != 'port':  # port tem default
                missing.append(key)
        
        if missing:
            print(f"❌ Variáveis de ambiente faltando: {missing}")
            return False
        return True
    
    @classmethod
    @contextmanager
    def get_connection(cls):
        """Context manager para conexões seguras com o banco"""
        if not cls.test_connection():
            raise ValueError("Configuração do banco incompleta!")
        
        conn = psycopg2.connect(**cls.DB_CONFIG)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()