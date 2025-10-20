# diagnostico_imports.py
import sys
from pathlib import Path

print("🔍 DIAGNÓSTICO DE IMPORTS")
print("=" * 50)

# 1. Verifica paths
project_root = Path(__file__).parent
print(f"📁 Diretório raiz: {project_root}")
print(f"📁 Python path: {sys.path}")

# 2. Verifica estrutura de arquivos
print("\n📋 ESTRUTURA DE ARQUIVOS:")
files = [
    ("src/__init__.py", project_root / "src" / "__init__.py"),
    ("src/config/__init__.py", project_root / "src" / "config" / "__init__.py"),
    ("src/config/database.py", project_root / "src" / "config" / "database.py"),
    (".env", project_root / ".env")
]

for name, path in files:
    if path.exists():
        print(f"   ✅ {name} - EXISTE ({path.stat().st_size} bytes)")
    else:
        print(f"   ❌ {name} - NÃO ENCONTRADO")

# 3. Tenta importar
print("\n🧪 TESTANDO IMPORTS:")
try:
    sys.path.insert(0, str(project_root))
    from src.config.database import DatabaseConfig
    print("   ✅ from src.config.database import DatabaseConfig - FUNCIONOU!")
    
    # Testa as variáveis de ambiente
    print("\n🔐 VARIÁVEIS DE AMBIENTE:")
    for key, value in DatabaseConfig.DB_CONFIG.items():
        if value:
            print(f"   ✅ {key}: {'*' * len(value)} (configurado)")
        else:
            print(f"   ❌ {key}: NÃO CONFIGURADO")
            
except ImportError as e:
    print(f"   ❌ Import falhou: {e}")

print("\n" + "=" * 50)