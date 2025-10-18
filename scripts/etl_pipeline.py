import pandas as pd
import numpy as np
from pathlib import Path
from src.config.database import DatabaseConfig
import logging

class HealthETLPipeline:

    """
    Pipeline ETL para dados de saúde de Curitiba.
    
    ETL = Extract, Transform, Load
    Esta classe orquestra todo o processo de dados.
    """

    def __init__(self):
        self.raw_data_path = Path('data/raw/saude')
        self.processed_data_path = Path('data/processed/')
        self.df = None # DataFrame principal onde trabalharemos
        self.stats = {}  # Para guardar estatísticas do processo

    def run(self):
        """
        Método principal que executa o pipeline completo.
        """
        print("🔄 Iniciando pipeline de saúde...")

        try:

            self.extract() #Extração

            self.transform() #Transformação

            self.load() #Carga

            self._print_statistics()
            print("✅ Pipeline de saúde concluído com sucesso!")

        except Exception as e:

            print(f"❌ Erro no pipeline de saúde: {e}")
            raise

    def extract(self):
        """
        Extrai os dados brutos dos arquivos CSV.
        """
        print("📥 Extraindo dados brutos...")

        # 1. Encontrar todos os arquivos CSV na pasta raw_data_path
        csv_files = list(self.raw_data_path.glob('*.csv'))

        if not csv_files:
            raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em {self.raw_data_path}")
        
        print(f"Encontrados {len(csv_files)} arquivos CSV.")

        # 2 Definir tipos para colunas de códigos (podem conter zeros à esquerda)
        dtype_spec = {
        'Código da Unidade': 'str',
        'Código do Procedimento': 'str', 
        'Código do CBO': 'str',
        'Código do CID': 'str',
        'CID do Internamento': 'str',
        'cod_usuario': 'str',           # Pode ter zeros à esquerda
        'cod_profissional': 'str'       # Pode ter zeros à esquerda
        }

        # 3. Ler e combinar todos os arquivos em um único DataFrame
        data_frames = []

        for csv_file in csv_files:
            print(f"Lendo arquivo: {csv_file.name}")

            # Ler CSV com configurações para dados brasileiros
        df_temp = pd.read_csv(
            csv_file,
            sep=';',               # Separador comum em CSVs BR
            encoding='latin-1',    # Encoding comum em dados BR  
            low_memory=False,      # Evita warnings de memória
            dtype=dtype_spec,      # códigos como string
            parse_dates=False      # Parse datas manual
        )
        
        data_frames.append(df_temp)

        # 4. Concatenar todos os DataFrames
        self.df = pd.concat(data_frames, ignore_index=True)

        # 5. Salvar estatísticas 
        self.stats['arquivos_processados'] = len(csv_files)
        self.stats['registros_extraidos'] = len(self.df)
        self.stats['colunas_extraídas'] = list(self.df.columns)

        # 6. Verificação de qualidade
        self._validate_data_quality()

    def  transform(self):
        """Fase 2: Limpeza e transformação dos dados"""
        print("🛠️  Fase 2 - Transformando dados...")
        
        # Ordem CRÍTICA das transformações
        self._convert_dates()           # 1. Datas primeiro
        self._convert_numeric()         # 2. Depois números
        self._handle_missing_values()   # 3. Tratar nulos
        self._create_derived_columns()  # 4. Novas colunas
        self._create_natural_key()      # 5. Chave única
        
        print("   ✅ Transformação concluída")
        self._validate_transformation()
        

    
    def load(self):
        """
        Carrega os dados transformados para o banco de dados.
        """
        print("💾 Carregando dados no banco...")


    def _validate_data_quality(self):
        """Faz verificações básicas de qualidade dos dados extraídos"""
        print("   🔍 Validando qualidade dos dados...")
        
        # Verificar se códigos importantes não foram convertidos para numéricos
        code_columns = ['Código da Unidade', 'Código do Procedimento','Código do CBO', 
                        'CID do Internamento', 'cod_usuario', 'cod_profissional', 'Código do CID']
        
        for col in code_columns:
            if col in self.df.columns:
                # Amostra dos primeiros valores
                sample = self.df[col].head(3).tolist()
                print(f"      {col}: {sample}")
                
                # Verificar se há zeros à esquerda
                if self.df[col].dtype == 'object':  # string
                    has_leading_zeros = self.df[col].astype(str).str.startswith('0').any()
                    if has_leading_zeros:
                        print(f"      ✅ {col} - Zeros à esquerda preservados")
                    else:
                        print(f"      ℹ️  {col} - Sem zeros à esquerda")


    def _print_statistics(self):
        """Exibe estatísticas do processo"""
        print("\n📊 Estatísticas do Processamento:")
        for key, value in self.stats.items():
            print(f"   {key}: {value}")