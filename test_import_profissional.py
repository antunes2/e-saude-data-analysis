#!/usr/bin/env python3
"""
Teste de imports PROFISSIONAL - sem hacks de sys.path
"""
try:
    # ✅ Importação limpa - como em projetos reais
    from scripts.etl_pipeline import HealthETLPipeline
    from src.config.database import DatabaseConfig
    
    print("✅ IMPORTS PROFISSIONAIS FUNCIONANDO!")
    print("   - from scripts.etl_pipeline import HealthETLPipeline")
    print("   - from src.config.database import DatabaseConfig")
    
    # Testa instanciação
    pipeline = HealthETLPipeline()
    print("✅ HealthETLPipeline instanciado com sucesso!")
    
    # Testa conexão
    with DatabaseConfig.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        print("✅ Conexão com banco funcionando!")
        
except ImportError as e:
    print(f"❌ Erro de import: {e}")
    print("\n💡 SOLUÇÃO: Execute no terminal:")
    print("   pip install -e .")
except Exception as e:
    print(f"❌ Outro erro: {e}")

print("\n🎯 Estrutura profissional configurada!")