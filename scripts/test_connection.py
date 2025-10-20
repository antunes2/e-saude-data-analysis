# test_database_env.py
from src.config.database import DatabaseConfig

def test_conexao_segura():
    """Testa a conexão com variáveis de ambiente"""
    print("🔐 Testando conexão segura...")
    
    # Primeiro testa se as variáveis estão configuradas
    if not DatabaseConfig.test_connection():
        print("💡 Dica: Crie um arquivo .env com:")
        print("   DB_HOST=localhost")
        print("   DB_NAME=eSaudeCuritiba") 
        print("   DB_USER=postgres")
        print("   DB_PASSWORD=sua_senha")
        return
    
    # Testa a conexão real
    try:
        with DatabaseConfig.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print(f"✅ Conexão segura OK! PostgreSQL: {version[0]}")
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")

if __name__ == "__main__":
    test_conexao_segura()