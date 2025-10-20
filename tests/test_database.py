
"""
Teste de conexão - VERSÃO PARA PASTA SCRIPTS
"""
import sys
import os
from pathlib import Path

# ✅ CORREÇÃO: Adiciona o diretório raiz ao Python path
current_dir = Path(__file__).parent
project_root = current_dir.parent  # Sobe um nível para a raiz
sys.path.insert(0, str(project_root))

try:
    from src.config.database import DatabaseConfig
    print("✅ DatabaseConfig importado com sucesso!")
except ImportError as e:
    print(f"❌ Erro ao importar: {e}")
    sys.exit(1)

def test_conexao():
    """Testa a conexão com o banco"""
    print("🔐 Testando conexão...")
    
    try:
        with DatabaseConfig.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print(f"✅ Conexão OK! PostgreSQL: {version[0]}")
            
            # Lista tabelas
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            print(f"📊 Tabelas encontradas: {len(tables)}")
            for table in tables:
                print(f"   - {table[0]}")
                
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")

if __name__ == "__main__":
    test_conexao()