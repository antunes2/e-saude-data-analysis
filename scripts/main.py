# scripts/etl.py
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import os

# ------------------------------
# 1. FUNÇÕES DE EXTRACTION (EXTRAIR)
# ------------------------------
def load_raw_data(data_path):
    """
    Carrega e concatena todos os arquivos CSV da pasta 'data/raw/'
    Retorna um único DataFrame.
    """
    all_files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.endswith('.csv')]
    df_list = []
    
    for file in all_files:
        # Lê cada CSV, cuidando com encoding e delimitador
        df_temp = pd.read_csv(file, sep=';', encoding='latin-1', low_memory=False)
        df_list.append(df_temp)
    
    # Concatena todos os DataFrames
    df_full = pd.concat(df_list, ignore_index=True)
    print(f"Dataset completo carregado. Shape: {df_full.shape}")
    return df_full

# ------------------------------
# 2. FUNÇÕES DE TRANSFORMATION (TRANSFORMAR)
# ------------------------------
def clean_data(df):
    """
    Faz a limpeza básica dos dados: corrige tipos, trata valores missing etc.
    Modifica o DataFrame inplace.
    """
    # Converte colunas para datetime
    date_cols = ['Data do Atendimento', 'Data de Nascimento']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Converte colunas numéricas (ex: onde vírgula é decimal)
    numeric_cols = ['Qtde Prescrita Farmácia Curitibana', 'Qtde Dispensada Farmácia Curitibana', 'Qtde de Medicamento Não Padronizado']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Preence valores missing em colunas categóricas importantes
    categorical_cols = ['Sexo', 'Solicitação de Exames', 'Encaminhamento para Atendimento Especialista']
    for col in categorical_cols:
        df[col].fillna('Não Informado', inplace=True)
    
    return df

def create_derived_columns(df):
    """
    Cria novas colunas (feature engineering) baseadas nas colunas existentes.
    """
    # Idade do paciente
    df['idade'] = (df['Data do Atendimento'] - df['Data de Nascimento']).dt.days // 365
    
    # Diferença entre prescrito e dispensado
    df['diff_prescrito_dispensado'] = df['Qtde Prescrita Farmácia Curitibana'] - df['Qtde Dispensada Farmácia Curitibana']
    
    # Flag para atendimento que gerou internação
    df['gerou_internamento'] = df['Desencadeou Internamento'].apply(lambda x: 1 if x == 'Sim' else 0)
    
    return df

def create_natural_key(df):
    """
    Cria a chave natural única para cada atendimento.
    Essencial para a carga incremental.
    """
    df['chave_natural'] = (
        df['Data do Atendimento'].astype(str) + 
        '_' + df['Código da Unidade'].astype(str) + 
        '_' + df['cod_usuario'].astype(str) + 
        '_' + df['Código do Procedimento'].astype(str)
    )
    return df

# ------------------------------
# 3. FUNÇÕES DE LOAD (CARREGAR)
# ------------------------------
def get_db_connection():
    """Cria e retorna uma conexão com o banco PostgreSQL."""
    conn = psycopg2.connect(
        host="localhost",
        database="eSaudeCuritiba",
        user="postgres",
        password="2042609hg"
    )
    return conn

def load_dimension_tables(df, conn):
    """
    Extrai dados para as tabelas dimensão e carrega no banco.
    Retorna dicionários com os mapeamentos de IDs gerados.
    """
    # Exemplo para dim_unidade
    dim_unidade = df[['Código da Unidade', 'Descrição da Unidade', 'Tipo de Unidade']].drop_duplicates()
    dim_unidade['unidade_id'] = range(1, len(dim_unidade) + 1)
    
    # Carrega no banco e retorna mapeamento {código: id}
    # (Implementar a inserção real aqui)
    unidade_map = dict(zip(dim_unidade['Código da Unidade'], dim_unidade['unidade_id']))
    
    return {'unidade': unidade_map}  # Retorna similar para outras dimensões

def load_fact_table(df, dimension_maps, conn):
    """
    Carrega os dados na tabela fato, usando os mapeamentos das dimensões.
    Implementa a lógica de carga incremental.
    """
    # Filtra apenas registros novos usando a chave natural
    existing_keys = pd.read_sql("SELECT chave_natural FROM fato_atendimento", conn)['chave_natural'].tolist()
    df_to_load = df[~df['chave_natural'].isin(existing_keys)]
    
    print(f"Carregando {len(df_to_load)} novos registros.")
    
    # Prepara DataFrame final para inserção
    # (Mapeia códigos para IDs das dimensões)
    
    # Insere no banco
    # (Implementar a inserção real aqui)

# ------------------------------
# 4. FUNÇÃO PRINCIPAL
# ------------------------------
def main():
    """Função principal que orquestra todo o pipeline ETL."""
    print("Iniciando pipeline ETL...")
    
    # Extract
    df = load_raw_data('data/raw/')
    
    # Transform
    df = clean_data(df)
    df = create_derived_columns(df)
    df = create_natural_key(df)
    
    print(f"Dataset após transformação. Shape: {df.shape}")
    
    # # Load
    # conn = get_db_connection()
    # try:
    #     dimension_maps = load_dimension_tables(df, conn)
    #     load_fact_table(df, dimension_maps, conn)
    #     conn.commit()
    #     print("Pipeline executado com sucesso!")
    # except Exception as e:
    #     conn.rollback()
    #     print(f"Erro durante a execução: {e}")
    # finally:
    #     conn.close()

if __name__ == "__main__":
    main()