import pandas as pd
import numpy as np
import os
from pathlib import Path
import openpyxl

def processar_arquivos_clima():
    # Configurar paths
    raw_folder = Path('data/raw/clima')
    processed_folder = Path('data/processed')
    processed_folder.mkdir(parents=True, exist_ok=True)
    
    # Listar arquivos CSV no diretório
    arquivos_xlsx = list(raw_folder.glob('*.xlsx'))
    
    if not arquivos_xlsx:
        print("Nenhum arquivo .xlsx encontrado em data/raw/clima/")
        return
    
    todos_dados = []
    
    for arquivo in arquivos_xlsx:
        print(f"Processando: {arquivo.name}")
        
        try:
            # Ler arquivo pulando as primeiras 8 linhas
            df = pd.read_excel(arquivo, engine='openpyxl')
            
            # Renomear colunas para facilitar (baseado na estrutura do exemplo)
            # Ajuste os nomes conforme necessário para seus arquivos
            colunas_map = {
                'Data': 'data_hora',
                'Hora UTC': 'hora_utc',
                'TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)': 'temp_max',
                'TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)': 'temp_min',
                'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'temp_atual'
            }
            
            df = df.rename(columns=colunas_map)
            
            # Converter data_hora para datetime
            df['data_hora'] = pd.to_datetime(df['data_hora'], errors='coerce')
            
            # Extrair apenas a data
            df['data'] = df['data_hora'].dt.date
            
            # Calcular temperatura média para cada registro
            def calcular_temp_media(row):
                # Prioridade: média entre max e min, depois max, depois min, depois atual
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
            
            df['temperatura_media'] = df.apply(calcular_temp_media, axis=1)
            
            # Remover linhas sem temperatura
            df = df.dropna(subset=['temperatura_media'])
            
            # Agrupar por dia para calcular média diária
            temp_diaria = df.groupby('data').agg({
                'temperatura_media': 'mean'
            }).reset_index()
            
            
            todos_dados.append(temp_diaria)
            
        except Exception as e:
            print(f"Erro ao processar {arquivo.name}: {e}")
            continue
    
    # Combinar todos os dados
    if todos_dados:
        dados_combinados = pd.concat(todos_dados, ignore_index=True)
        
        # Salvar resultado
        output_path = processed_folder / 'temperatura_diaria.csv'
        dados_combinados.to_csv(output_path, index=False)
        print(f"Dados processados salvos em: {output_path}")
        
        # Estatísticas
        print(f"\nEstatísticas:")
        print(f"Total de dias processados: {len(dados_combinados)}")
        print(f"Período: {dados_combinados['data'].min()} até {dados_combinados['data'].max()}")
        print(f"Temperatura média geral: {dados_combinados['temperatura_media'].mean():.2f}°C")
        
        return dados_combinados
    else:
        print("Nenhum dado foi processado com sucesso.")
        return None

# Executar o processamento
if __name__ == "__main__":
    dados_processados = processar_arquivos_clima()