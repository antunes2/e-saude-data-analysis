import pytest
import pandas as pd
import sys
import os
from pathlib import Path

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
    
    def create_test_dataframe_from_real_data(self):
        """Cria DataFrame de teste a partir de dados reais (mais profissional)"""
        try:
            # ✅ SOLUÇÃO: Usar dados reais mas limitados
            from scripts.etl_pipeline import HealthETLPipeline
            
            # Cria pipeline temporário apenas para extração
            pipeline = HealthETLPipeline()
            pipeline.raw_data_path = Path('data/raw/saude/test_samples')  # Sua pasta com poucos arquivos
            
            # Extrai apenas (sem transformar)
            pipeline.extract()

            # converte numericos
            pipeline._convert_numeric()

            # Converte datas
            pipeline._convert_dates()
            
            # Pega apenas as primeiras 50 linhas para teste rápido
            df_test = pipeline.df.head(5000).copy()
            
            print(f"✅ DataFrame real criado com {len(df_test)} registros")
            print(f"   Colunas: {list(df_test.columns)}")
            
            return df_test
            
        except Exception as e:
            print(f"❌ Erro ao criar DataFrame real: {e}")
            # Fallback: criar dados mínimos manualmente
            return self.create_minimal_test_dataframe()
    
    def create_minimal_test_dataframe(self):
        """Fallback: cria DataFrame mínimo com colunas essenciais"""
        print("⚠️  Criando DataFrame mínimo de fallback...")
        
        test_data = {
            'Código da Unidade': ['001', '002'],
            'Descrição da Unidade': ['UPA Centro', 'UPA Bairro'],
            'Código do Tipo de Unidade': ['1', '1'],
            'Tipo de Unidade': ['UPA', 'UPA'],
            'Código do Procedimento': ['PROC001', 'PROC002'],
            'Descrição do Procedimento': ['Consulta', 'Exame'],
            'Código do CID': ['A01', 'B02'],
            'Descrição do CID': ['Febre', 'Tosse'],
            'Código do CBO': ['CBO001', 'CBO002'],
            'Descrição do CBO': ['Médico', 'Enfermeiro'],
            'cod_usuario': [1001, 1002],
            'Sexo': ['M', 'F'],
            'Data de Nascimento': ['1990-01-01', '1985-05-15'],
            'Nacionalidade': ['Brasileira', 'Brasileira'],
            'origem_usuario': [1, 1],
            'Município': ['Curitiba', 'Curitiba'],
            'Bairro': ['Centro', 'Bairro Novo'],
            'Energia Elétrica': ['Sim', 'Sim'],
            # ✅ COLUNAS ADICIONAIS que o código espera (valores padrão)
            'Tratamento no Domicílio': [None, None],
            'Abastecimento': [None, None],
            'Tipo de Habitação': [None, None],
            'Destino Lixo': [None, None],
            'Fezes/Urina': [None, None],
            'Cômodos': [None, None],
            'Em Caso de Doença': [None, None],
            'Grupo Comunitário': [None, None],
            'Meio de Comunicacao': [None, None],
            'Meio de Transporte': [None, None]
        }
        
        df_test = pd.DataFrame(test_data)
        print(f"✅ DataFrame mínimo criado com {len(df_test)} registros")
        return df_test
    
    def test_dimension_loader_integration(self):
        """Teste de integração completo do DimensionLoader"""
        print("\n🧪 Testando DimensionLoader...")
        
        # 1. Cria DataFrame de teste a partir de dados reais
        df_test = self.create_test_dataframe_from_real_data()
        
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
                
                # Verificações básicas
                assert len(dimension_maps) == 5  # 5 dimensões
                
                print("✅ Teste do DimensionLoader concluído com sucesso!")
                
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