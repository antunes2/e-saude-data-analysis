import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Optional
from src.config.database import DatabaseConfig
import logging

class FactLoader:

    """
    Specialist em carregar tabelas dimensão no PostgreSQL.
    Gerencia a carga de todas as dimensões e mantém mapeamentos de IDs.
    """

    def __init__(self, dimension_maps):
        self.dimension_maps = dimension_maps
        self.logger = logging.getLogger(__name__)

    def load_fato_atendimento(self, df: pd.DataFrame, conn) -> None:
        """
        Carrega a tabela fato_atendimento no banco de dados.
        Usa os mapeamentos de dimensão para substituir valores por IDs.
        """

        cursor = conn.cursor()
        
        inseridos = 0
        duplicados = 0
        erros = 0
        
        print("📊 Carregando tabela fato...")

        # ✅ DEBUG CRÍTICO: Verificar tipos de dados na PRIMEIRA linha
        primeira_linha = df.iloc[0]
        print("🔍 DEBUG - Tipos de dados na primeira linha:")
        print(f"   'cod_usuario' type: {type(primeira_linha['cod_usuario'])}, value: {primeira_linha['cod_usuario']}")
        print(f"   'Código da Unidade' type: {type(primeira_linha['Código da Unidade'])}, value: {primeira_linha['Código da Unidade']}")
        
        # ✅ DEBUG: Verificar tipos no dimension_maps
        print("🔍 DEBUG - Tipos no dimension_maps:")
        perfil_sample_key = list(self.dimension_maps['perfil'].keys())[0]
        print(f"   dimension_maps['perfil'] key type: {type(perfil_sample_key)}, value: {perfil_sample_key}")
        
        error_types = {'unidade': 0, 'procedimento': 0, 'cid': 0, 'cbo': 0, 'perfil': 0}

        for index, row in df.iterrows():
            # Mostrar progresso a cada 10.000 linhas
            if index % 15000 == 0 and index > 0:
                print(f"   📈 Processadas {index} linhas fact_loader...")
            
            try:

                # Tratar código CID "Não Informado"
                if pd.isna(row['Código do CID']) or row['Código do CID'] in ['', 'None', None, 'NaN']:
                    codigo_cid = 'NI'  # Usa o código "Não Informado"
                else:
                    codigo_cid = str(row['Código do CID']).strip()

                # ✅ CONVERTER para os mesmos tipos do dimension_maps
                codigo_unidade = str(row['Código da Unidade']) if pd.notna(row['Código da Unidade']) else None
                codigo_procedimento = str(row['Código do Procedimento']) if pd.notna(row['Código do Procedimento']) else None
                codigo_cbo = str(row['Código do CBO']) if pd.notna(row['Código do CBO']) else None
                codigo_usuario = str(row['cod_usuario']) if pd.notna(row['cod_usuario']) else None  

                # Se qualquer FK for none, pular
                if None in [codigo_unidade, codigo_procedimento, codigo_cid, codigo_cbo, codigo_usuario]:
                    erros += 1
                    if erros <= 7:  # Mostrar apenas os primeiros 10 erros
                        print(f"   ❌  Linha {index}: Código None encontrado - pulando")

                # códigos naturais -> IDs de dimensão
                unidade_id = self.dimension_maps['unidade'].get(codigo_unidade)
                procedimento_id = self.dimension_maps['procedimento'].get(codigo_procedimento)
                cid_id = self.dimension_maps['cid'].get(codigo_cid)
                cbo_id = self.dimension_maps['cbo'].get(codigo_cbo)
                perfil_id = self.dimension_maps['perfil'].get(int(codigo_usuario)) # <- converte para int na hora da busca

                # ✅ VALIDAR e IDENTIFICAR qual FK está faltando
                missing_fks = []
                if unidade_id is None:
                    missing_fks.append('unidade')
                    error_types['unidade'] += 1
                if procedimento_id is None:
                    missing_fks.append('procedimento') 
                    error_types['procedimento'] += 1
                if cid_id is None:
                    missing_fks.append('cid')
                    error_types['cid'] += 1
                if cbo_id is None:
                    missing_fks.append('cbo')
                    error_types['cbo'] += 1  
                if perfil_id is None:
                    missing_fks.append('perfil')
                    error_types['perfil'] += 1
                
                if missing_fks:
                    # Mostrar apenas algumas linhas de erro para debug
                    if erros < 10:  # Mostra apenas os primeiros 10 erros
                        print(f"   ❌ Linha {index}: FKs faltando - {missing_fks}")
                        print(f"      Valores: unidade={row['Código da Unidade']}, procedimento={row['Código do Procedimento']}, cid={row['Código do CID']}, cbo={row['Código do CBO']}, usuario={row['cod_usuario']}")
                    erros += 1
                    continue

                # Inserir na tabela fato (apenas IDs e medidas)
                cursor.execute("""
                    INSERT INTO fato_atendimento (
                        unidade_id, procedimento_id, cid_id, cbo_id, perfil_id,
                        qtde_prescrita, qtde_dispensada, qtde_nao_padronizado,
                        idade_paciente, diff_prescrito_dispensado, gerou_internamento,
                        data_atendimento, morador_curitiba_rm, periodo_dia, faixa_etaria,
                        -- ✅ NOVAS COLUNAS
                        estabelecimento_solicitante, estabelecimento_destino,
                        solicitacao_exames, encaminhamento_especialista,
                        chave_natural
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chave_natural) DO NOTHING
                    """, (
                    unidade_id, procedimento_id, cid_id, cbo_id, perfil_id,
                    row['Qtde Prescrita Farmácia Curitibana'],
                    row['Qtde Dispensada Farmácia Curitibana'],
                    row['Qtde de Medicamento Não Padronizado'],
                    row['idade'],
                    row['diff_prescrito_dispensado'],
                    row['gerou_internamento'],
                    row['Data do Atendimento'],
                    row['morador_curitiba_rm'],
                    row['periodo_dia'],
                    row['faixa_etaria'],
                    # ✅ NOVOS VALORES
                    row.get('Estabelecimento Solicitante'),
                    row.get('Estabelecimento Destino'),
                    row.get('Solicitação de Exames'),
                    row.get('Encaminhamento para Atendimento Especialista'),
                    row['chave_natural']
                    ))
                
                if cursor.rowcount > 0:
                    inseridos += 1
                else:
                    duplicados += 1
                
            except Exception as e:
                self.logger.error(f"Erro ao inserir linha {row['chave_natural']}: {e}")
                erros += 1

        conn.commit()
        print(f"✅ Carga concluída: {inseridos} inseridos, {duplicados} duplicados, {erros} erros.")