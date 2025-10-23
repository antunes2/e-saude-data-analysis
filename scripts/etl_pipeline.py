import pandas as pd
import numpy as np
from pathlib import Path
from src.config.database import DatabaseConfig
from scripts.loaders.dimension_loader import DimensionLoader
import logging
from datetime import date


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

        # 4. Concatenar todos os DataFrames e renomear Município
        self.df = pd.concat(data_frames, ignore_index=True)
        if 'Municício' in self.df.columns:
            self.df.rename(columns={'Municício': 'Município'}, inplace=True)
    
        # 🚀 MELHORIA FUTURA: Para projetos maiores, criar função _standardize_column_names()
        # que gerencia múltiplas inconsistências de nomenclatura automaticamente

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
        
    def _convert_dates(self):
        """Converte colunas de data para datetime"""
        date_cols = ['Data do Atendimento', 'Data de Nascimento']
        
        for col in date_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(
                    self.df[col], 
                    format='%d/%m/%Y %H:%M:%S',
                    errors='coerce'
                    )
        
        print("  🔄 Datas convertidas para datetime")

    def _convert_numeric(self):
        """Trata valores missing e converte numéricos SIMPLES"""
    
        # Colunas que queremos como inteiros
        int_columns = [
            'Qtde Prescrita Farmácia Curitibana',
            'Qtde Dispensada Farmácia Curitibana', 
            'Qtde de Medicamento Não Padronizado',
            'Cômodos'
        ]
        
        for col in int_columns:
            if col in self.df.columns:
                # Converte para numérico, trata erros, depois para inteiro
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                self.df[col] = self.df[col].fillna(0).astype(int)
                print(f"   ✅ {col} convertida para inteiro")
    
    def _handle_missing_values(self):
        """Trata valores missing em colunas categóricas importantes"""
        
        print("  🔄 Tratando valores missing...")

        total_nulos_inicial = self.df.isnull().sum().sum()

        # Apenas colunas que NÃO podem ser NULL na tabela fato
        CRITICAL_COLUMNS = {
            'Sexo': 'Não Informado',
            'Solicitação de Exames': 'Não Informado',
            'Encaminhamento para Atendimento Especialista': 'Não Informado', 
            'Desencadeou Internamento': 'Não Informado'}
        
        for col, fill_value in CRITICAL_COLUMNS.items():
            if col in self.df_columns:
                n_nulos = self.df[col].isna().sum()
                if n_nulos > 0:
                    self.df[col] = self.df[col].fillna(fill_value)
                    print(f"      ✅ {col}: {n_nulos} nulos → '{fill_value}'")
                else:
                    print(f"      ℹ️  {col}: sem nulos")

        # Validaçao da integridade de dados
        total_nulos_final = self.df.isna().sum().sum()

        print(f"   🔍 Total de valores missing antes: {total_nulos_inicial}, depois: {total_nulos_final}")

        # Validaçao final
        self._validate_data_integrity()

    def _create_derived_columns(self):
        """Cria novas colunas derivadas (feature engineering)"""
        print("  🔄 Criando colunas derivadas...")
        
        # Idade do paciente
        self.df['idade'] = (self.df['Data do Atendimento'] - self.df['Data de Nascimento']).dt.days // 365
        
        # Diferença entre prescrito e dispensado
        self.df['diff_prescrito_dispensado'] = self.df['Qtde Prescrita Farmácia Curitibana'] - self.df['Qtde Dispensada Farmácia Curitibana']
        
        # Flag para atendimento que gerou internação
        self.df['gerou_internamento'] = self.df['Desencadeou Internamento'].apply(lambda x: 1 if x == 'Sim' else 0)
        
        # Flag para morador de Curitiba ou região metropolitana
        self.df['morador_curitiba_rm'] = self.df['Município'].apply(lambda x: "Curitiba" if x == "Curitiba" else "Região Metropolitana")

        # Perídodo do dia do atendimento
        self.df['periodo_dia'] = self.df['Data do Atendimento'].dt.hour.apply(
        lambda x: 'Manhã' if 6 <= x < 12 else 
                  'Tarde' if 12 <= x < 18 else 
                  'Noite' if 18 <= x < 24 else 'Madrugada')
        
        # Faixa etária
        self.df['faixa_etaria'] = self.df['idade'].apply(
        lambda x: 'Criança' if x <= 12 else 
                  'Adolescente' if x <= 19 else 
                  'Adulto' if x <= 59 else 'Idoso'
    )

        print("   ✅ Colunas derivadas criadas")

    def _create_natural_key(self):
        """Cria a chave natural única para cada atendimento."""
        print("  🔄 Criando chave natural única...")
        
        self.df['chave_natural'] = (
            self.df['Data do Atendimento'].astype(str) + 
            '_' + self.df['Código da Unidade'].astype(str) + 
            '_' + self.df['cod_usuario'].astype(str) + 
            '_' + self.df['Código do Procedimento'].astype(str)
        )
        
        # Valida se realmente é única
        total_registros = len(self.df)
        registros_unicos = self.df['chave_natural'].nunique()

        print(f"      📊 Registros: {total_registros:,}")
        print(f"      🔑 Chaves únicas: {registros_unicos:,}")

        if total_registros != registros_unicos:
            raise ValueError(f"Chave natural não é única! Total registros: {total_registros}, únicos: {registros_unicos}")
        else:
            print("      ✅ Chave natural é única!")


    def load(self):
        """
        Carrega os dados transformados para o banco de dados.
        """
        print("💾 Carregando dados no banco...")

        try:
            with DatabaseConfig.get_connection() as conn:
                # 1. Carregar dimensoes primeiro
                dimension_loader = DimensionLoader()
                dimension_maps = dimension_loader.load_all(self.df, conn)

                # 2. Guardar os mapeamentos para usar na tabela fato
                self.dimension_maps = dimension_maps

                # 3. Estatísticas
                self.starts['dimensoes_carregadas'] = len(dimension_maps)
                for dim_name, mapping in dimension_maps.items():
                    self.stats[f'registros_{dim_name}_inseridos'] = len(mapping)

                print("   ✅ Dimensões carregadas com sucesso")

        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
            raise


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

    def _validate_data_integrity(self):
        """Valida que colunas essenciais estão preenchidas"""
        print("   🔍 Validando integridade dos dados...")
        
        ESSENTIAL_COLUMNS = [
            'Data do Atendimento',
            'Data de Nascimento',
            'Sexo', 
            'Código da Unidade',
            'Código do Procedimento',
            'Código do CID',
            'Código do CBO',
            'cod_usuario'
        ]
        
        issues = []
        for col in ESSENTIAL_COLUMNS:
            if col in self.df.columns:
                n_nulos = self.df[col].isna().sum()
                if n_nulos > 0:
                    issues.append(f"{col}: {n_nulos} nulos")
                else:
                    print(f"      ✅ {col}: Completo")
        
        if issues:
            print(f"      ⚠️  Problemas encontrados: {', '.join(issues)}")
        else:
            print("      ✅ Todas colunas essenciais estão completas!") 

    def _print_statistics(self):
        """Exibe estatísticas do processo"""
        print("\n📊 Estatísticas do Processamento:")
        for key, value in self.stats.items():
            print(f"   {key}: {value}")