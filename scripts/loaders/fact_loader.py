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

        for index, row in df.iterrows():
            try:
                # códigos naturais -> IDs de dimensão
                unidade_id = self.dimension_maps['unidade'].get(row['Código da Unidade'])
                procedimento_id = self.dimension_maps['procedimento'].get(row['Código do Procedimento'])
                cid_id = self.dimension_maps['cid'].get(row['Código do CID'])
                cbo_id = self.dimension_maps['cbo'].get(row['Código do CBO'])
                perfil_id = self.dimension_maps['perfil'].get(row['cod_usuario'])

                # Validar se as FK existem
                if None in [unidade_id, procedimento_id, cid_id, cbo_id, perfil_id]:
                    self.logger.warning(f"FK não encontrada para linha {index}, pulando...")
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