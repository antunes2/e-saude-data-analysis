import pandas as pd
import numpy as np
from pathlib import Path
from src.config.database import DatabaseConfig
from datetime import date

class ClimateETLPipeline:
    """
    Pipeline ETL para dados climáticos.
    Processa arquivos XLSX de temperatura e carrega no PostgreSQL.
    """
    
    def __init__(self):
        self.raw_data_path = Path('data/raw/clima')
        self.processed_data_path = Path('data/processed')
        self.df = None
        self.stats = {}
        
        # Mapeamento de colunas baseado no seu código
        self.column_mapping = {
            'Data': 'data_hora',
            'Hora UTC': 'hora_utc', 
            'TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)': 'temp_max',
            'TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)': 'temp_min',
            'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'temp_atual'
        }
    
    def run(self):
        """Executa o pipeline climático completo"""
        print("🌤️  Iniciando pipeline climático...")
        
        try:
            self.extract()
            self.transform() 
            self.load()
            
            self._print_statistics()
            print("✅ Pipeline climático concluído!")
            
        except Exception as e:
            print(f"❌ Erro no pipeline climático: {e}")
            raise
    
    def extract(self):
        """Extrai dados dos arquivos XLSX"""
        print("📥 Fase 1 - Extraindo dados climáticos...")
        
        csv_files = list(self.raw_data_path.glob('*.csv'))
        
        if not csv_files:
            raise FileNotFoundError(f"Nenhum CSV encontrado em {self.raw_data_path}")
        
        print(f"   Encontrados {len(csv_files)} arquivos CSV")
        
        dataframes = []
        for csv_file in csv_files:
            print(f"   Lendo: {csv_file.name}")
            
            df_temp = pd.read_csv(
                csv_file,
                skiprows=10,
                sep=';',
                encoding='latin-1',
                low_memory= False
            )
            
            dataframes.append(df_temp)
        
        # Combinar todos os DataFrames
        self.df = pd.concat(dataframes, ignore_index=True)
        self.stats['arquivos_clima_processados'] = len(csv_files)
        self.stats['registros_clima_extraidos'] = len(self.df)
    
    def transform(self):
        """Transforma dados climáticos - baseado no seu código original"""
        print("🛠️  Fase 2 - Transformando dados climáticos...")
        
        # 1. Renomear colunas
        self.df = self.df.rename(columns=self.column_mapping)
        
        # 2. Converter data_hora
        self.df['data_hora'] = pd.to_datetime(self.df['data_hora'], errors='coerce')
        
        # 3. Extrair apenas a data
        self.df['data'] = self.df['data_hora'].dt.date
        
        # 4. Calcular temperatura média (usando sua lógica original)
        def calcular_temp_media(row):
            if pd.notna(row['temp_max']) and pd.notna(row['temp_min']):
                return (row['temp_max'] + row['temp_min']) / 2
            elif pd.notna(row['temp_max']):
                return row['temp_max']
            elif pd.notna(row['temp_min']):
                return row['temp_min']
            elif pd.notna(row['temp_atual']):
                return row['temp_atual']
            else:
                return np.nan
        
        self.df['temperatura_media'] = self.df.apply(calcular_temp_media, axis=1)
        
        # 5. Remover linhas sem temperatura
        self.df = self.df.dropna(subset=['temperatura_media'])
        
        # 6. Agrupar por dia para calcular média diária
        temp_diaria = self.df.groupby('data').agg({
            'temperatura_media': 'mean'
        }).reset_index()
        
        self.df = temp_diaria  # Agora temos dados diários
        self.stats['dias_clima_processados'] = len(self.df)
    
    def load(self):
        """Carrega dados climáticos no PostgreSQL"""
        print("📤 Fase 3 - Carregando dados climáticos...")
        
        with DatabaseConfig.get_connection() as conn:
            cursor = conn.cursor()
            
            for _, row in self.df.iterrows():
                # Inserir ou atualizar dados na dim_temperatura
                query = """
                INSERT INTO dim_temperatura (data, temperatura_media)
                VALUES (%s, %s)
                ON CONFLICT (data) 
                DO UPDATE SET temperatura_media = EXCLUDED.temperatura_media;
                """
                
                cursor.execute(query, (row['data'], float(row['temperatura_media'])))
            
            self.stats['dias_clima_inseridos'] = len(self.df)
    
    def _print_statistics(self):
        print("\n🌡️  Estatísticas Climáticas:")
        for key, value in self.stats.items():
            print(f"   {key}: {value}")