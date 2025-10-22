import pytest
import pandas as pd
import sys
import os

# Adiciona o diretório raiz ao path para importar os módulos
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from scripts.loaders.dimension_loader import DimensionLoader
from src.config.database import DatabaseConfig

class TestDimensionLoader:
    """Testes para o DimensionLoader"""
    
    def test_loader_initialization(self):
        """Testa se o DimensionLoader inicializa corretamente"""
        loader = DimensionLoader()
        
        # Verifica se os mapeamentos estão vazios no início
        assert isinstance(loader.dimension_maps, dict)
        assert 'unidade' in loader.dimension_maps
        assert 'procedimento' in loader.dimension_maps
        assert 'cid' in loader.dimension_maps
        assert 'cbo' in loader.dimension_maps
        assert 'perfil' in loader.dimension_maps
        
        print("✅ DimensionLoader inicializa corretamente")
    
    def test_create_test_dataframe(self):
        """Cria um DataFrame de teste pequeno"""
        test_data = {
            'Código da Unidade': ['001', '002', '001'],  # Duplicado proposital
            'Descrição da Unidade': ['UPA Centro', 'UPA Bairro', 'UPA Centro'],
            'Código do Tipo de Unidade': ['1', '1', '1'],
            'Tipo de Unidade': ['UPA', 'UPA', 'UPA'],
            'Código do Procedimento': ['PROC001', 'PROC002', 'PROC001'],
            'Descrição do Procedimento': ['Consulta', 'Exame', 'Consulta'],
            'Código do CID': ['A01', 'B02', 'A01'],
            'Descrição do CID': ['Febre', 'Tosse', 'Febre'],
            'Código do CBO': ['CBO001', 'CBO002', 'CBO001'],
            'Descrição do CBO': ['Médico', 'Enfermeiro', 'Médico'],
            'cod_usuario': [1001, 1002, 1001],  # Duplicado proposital
            'Sexo': ['M', 'F', 'M'],
            'Data de Nascimento': ['1990-01-01', '1985-05-15', '1990-01-01'],
            'Nacionalidade': ['Brasileira', 'Brasileira', 'Brasileira'],
            'origem_usuario': [1, 1, 1],
            'Município': ['Curitiba', 'Curitiba', 'Curitiba'],
            'Bairro': ['Centro', 'Bairro Novo', 'Centro'],
            'Energia Elétrica': ['Sim', 'Sim', 'Sim']
        }
        
        df_test = pd.DataFrame(test_data)
        print(f"✅ DataFrame de teste criado com {len(df_test)} registros")
        return df_test
    
    def test_dimension_loader_integration(self):
        """Teste de integração completo do DimensionLoader"""
        print("\n🧪 Testando DimensionLoader...")
        
        # 1. Cria DataFrame de teste
        df_test = self.test_create_test_dataframe()
        
        # 2. Conecta ao banco
        try:
            with DatabaseConfig.get_connection() as conn:
                # 3. Cria loader
                loader = DimensionLoader()
                
                # 4. Executa carga de dimensões
                dimension_maps = loader.load_all(df_test, conn)
                
                # 5. Verifica resultados
                print("\n📊 Resultados do DimensionLoader:")
                for dim_name, mapping in dimension_maps.items():
                    print(f"   {dim_name}: {len(mapping)} registros mapeados")
                
                # Verificações
                assert len(dimension_maps['unidade']) == 2  # 2 unidades únicas
                assert len(dimension_maps['procedimento']) == 2  # 2 procedimentos únicos
                assert len(dimension_maps['cid']) == 2  # 2 CIDs únicos
                assert len(dimension_maps['cbo']) == 2  # 2 CBOs únicos
                assert len(dimension_maps['perfil']) == 2  # 2 perfis únicos
                
                print("✅ Todos os testes do DimensionLoader passaram!")
                
                return dimension_maps
                
        except Exception as e:
            print(f"❌ Erro no teste: {e}")
            raise

def run_dimension_loader_test():
    """Função para executar o teste manualmente"""
    tester = TestDimensionLoader()
    return tester.test_dimension_loader_integration()

if __name__ == "__main__":
    run_dimension_loader_test()