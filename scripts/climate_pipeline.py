import pandas as pd
import numpy as np
from pathlib import Path
from src.config.database import DatabaseConfig

class ClimateETLPipeline:
    """
    Pipeline ETL para dados climáticos.
    Processa arquivos CSV de temperatura e carrega no PostgreSQL.
    """
    
    def __init__(self, data_path=None):
        if data_path:
            self.raw_data_path = Path(data_path)
        else:
            self.project_root = Path(__file__).parent.parent
            self.raw_data_path = self.project_root / 'data' / 'raw' / 'clima'
        
        self.df = None
        self.stats = {}
        
        self.column_mapping = {
            'Data Medicao': 'data_medicao',
            'Hora Medicao': 'hora_medicao',
            'TEMPERATURA DO AR - BULBO SECO, HORARIA(Â°C)': 'temp_bulbo_seco',
            'TEMPERATURA MAXIMA NA HORA ANT. (AUT)(Â°C)': 'temp_max',
            'TEMPERATURA MINIMA NA HORA ANT. (AUT)(Â°C)': 'temp_min'
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
        """Extrai dados dos arquivos CSV"""
        print("📥 Extraindo dados climáticos...")
        
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
                decimal='.',
                na_values=['null', 'NULL', ''],
                low_memory=False
            )
            
            dataframes.append(df_temp)
        
        self.df = pd.concat(dataframes, ignore_index=True)
        self.stats['arquivos_processados'] = len(csv_files)
        self.stats['registros_extraidos'] = len(self.df)
    
    def transform(self):
        """Transforma dados climáticos"""
        print("🛠️  Transformando dados climáticos...")
        
        # 1. Renomear colunas
        self.df = self.df.rename(columns=self.column_mapping)
        
        # 2. Converter temperaturas para numérico
        for col in ['temp_bulbo_seco', 'temp_max', 'temp_min']:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
        
        # 3. Combinar data e hora
        self.df['hora_medicao'] = self.df['hora_medicao'].astype(str).str.zfill(4)
        
        self.df['data_hora'] = pd.to_datetime(
            self.df['data_medicao'] + ' ' + 
            self.df['hora_medicao'].str[:2] + ':' + 
            self.df['hora_medicao'].str[2:],
            format='%Y-%m-%d %H:%M',
            errors='coerce'
        )
        
        # 4. Extrair data
        self.df['data'] = self.df['data_hora'].dt.date
        
        # 5. Calcular temperatura média horária
        def calcular_temp_media(row):
            if pd.notna(row['temp_max']) and pd.notna(row['temp_min']):
                return (row['temp_max'] + row['temp_min']) / 2
            elif pd.notna(row['temp_bulbo_seco']):
                return row['temp_bulbo_seco']
            else:
                return np.nan
        
        self.df['temperatura_media_horaria'] = self.df.apply(calcular_temp_media, axis=1)
        
        # 6. Remover linhas sem temperatura
        initial_count = len(self.df)
        self.df = self.df.dropna(subset=['temperatura_media_horaria'])
        self.stats['registros_sem_temperatura'] = initial_count - len(self.df)
        
        # 7. Agrupar por dia para média diária
        temp_diaria = self.df.groupby('data').agg({
            'temperatura_media_horaria': 'mean'
        }).reset_index()
        
        temp_diaria = temp_diaria.rename(columns={
            'temperatura_media_horaria': 'temperatura_media'
        })
        
        self.df = temp_diaria
        self.stats['dias_processados'] = len(self.df)
    
    def load(self):
        """Carrega dados climáticos no PostgreSQL"""
        print("📤 Carregando dados climáticos...")
        
        if self.df.empty:
            print("   ⚠️  Nenhum dado para carregar")
            return
        
        with DatabaseConfig.get_connection() as conn:
            cursor = conn.cursor()
            
            for _, row in self.df.iterrows():
                query = """
                INSERT INTO dim_temperatura (data, temperatura_media)
                VALUES (%s, %s)
                ON CONFLICT (data) 
                DO UPDATE SET 
                    temperatura_media = EXCLUDED.temperatura_media
                """
                
                cursor.execute(query, (
                    row['data'], 
                    float(row['temperatura_media'])
                ))
            
            self.stats['dias_inseridos'] = len(self.df)
    
    def _print_statistics(self):
        """Exibe estatísticas básicas"""
        print("\n📊 Estatísticas do Processamento:")
        for key, value in self.stats.items():
            print(f"   {key}: {value}")