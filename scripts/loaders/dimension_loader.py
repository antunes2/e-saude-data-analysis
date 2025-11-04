import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Optional
from src.config.database import DatabaseConfig
import logging

class DimensionLoader:
    """
    Specialist em carregar tabelas dimensão no PostgreSQL.
    Gerencia a carga de todas as dimensões e mantém mapeamentos de IDs.
    """
    
    def __init__(self):
        self.dimension_maps: Dict[str, Dict] = {
            'unidade': {},      # Mapeia codigo_unidade -> unidade_id
            'procedimento': {}, # Mapeia codigo_procedimento -> procedimento_id  
            'cid': {},          # Mapeia codigo_cid -> cid_id
            'cbo': {},          # Mapeia codigo_cbo -> cbo_id
            'perfil': {}        # Mapeia cod_usuario -> perfil_id
        }
        self.logger = logging.getLogger(__name__)
    
    def load_all(self, df: pd.DataFrame, conn) -> Dict[str, Dict]:
        """
        Carrega todas as dimensões na ordem correta.
        
        Args:
            df: DataFrame com dados transformados
            conn: Conexão PostgreSQL
            
        Returns:
            Dicionário com mapeamentos de IDs gerados
        """
        print("📥 Iniciando carga de dimensões...")
        
        # Ordem CRÍTICA - algumas dimensões podem depender de outras
        self.load_unidades(df, conn)
        self.load_procedimentos(df, conn)
        self.load_cids(df, conn) 
        self.load_cbos(df, conn)
        self.load_perfis(df, conn)

        # ✅ DEBUG: Verificar o que foi realmente carregado
        print("\n🔍 DEBUG - Dimension Maps carregados:")
        for dim_name, mapping in self.dimension_maps.items():
            print(f"   {dim_name}: {len(mapping)} registros")
            if mapping:  # Se não estiver vazio, mostra alguns exemplos
                sample_items = list(mapping.items())[:3]  # Primeiros 3 itens
                print(f"      Amostra: {sample_items}")
        
        print("✅ Todas dimensões carregadas!")
        return self.dimension_maps
    
    def load_unidades(self, df: pd.DataFrame, conn) -> None:
        """Carrega dim_unidade com dados únicos"""
        cursor = conn.cursor()

        # Extrai dados unicos de unidade
        colunas_unidade = ['Código da Unidade', 'Descrição da Unidade', 'Código do Tipo de Unidade', 'Tipo de Unidade',]
        dim_unidade = df[colunas_unidade].drop_duplicates()

        # Contador para debug
        inseridas = 0
        existentes = 0

        for _, row in dim_unidade.iterrows():
            cursor.execute("""
                           INSERT INTO dim_unidade (codigo_unidade, descricao_unidade, codigo_tipo_unidade, tipo_unidade)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (codigo_unidade) DO NOTHING
                           RETURNING unidade_id, codigo_unidade;""",
                           (row['Código da Unidade'], 
                            row['Descrição da Unidade'],
                            row['Código do Tipo de Unidade'], 
                            row['Tipo de Unidade']))
        
            # Atualiza o mapeamento
            result = cursor.fetchone()
            
            if result:
                unidade_id, codigo_unidade = result
                self.dimension_maps['unidade'][codigo_unidade] = unidade_id
                inseridas += 1
            else:
                existentes +=1

        conn.commit()

        self.logger.info(f"📥 dim_unidade: {inseridas} novas, {existentes} existentes")
        print("      ✅ Dimensão unidade carregada com sucesso!")
    
    def load_procedimentos(self, df: pd.DataFrame, conn) -> None:
        """Carrega dim_procedimento com dados únicos"""
        
        cursor = conn.cursor()

        # Extrai dados unicos de procedimento
        colunas_procedimento = ['Código do Procedimento', 'Descrição do Procedimento']
        dim_procedimento = df[colunas_procedimento].drop_duplicates()

        # Contador para debug
        inseridas = 0
        existentes = 0

        for _, row in dim_procedimento.iterrows():
            cursor.execute("""
                           INSERT INTO dim_procedimento (codigo_procedimento, descricao_procedimento)
                            VALUES (%s, %s)
                            ON CONFLICT (codigo_procedimento) DO NOTHING
                           RETURNING procedimento_id, codigo_procedimento;""",
                           (row['Código do Procedimento'], 
                            row['Descrição do Procedimento']))
            
            result = cursor.fetchone()
            if result:
                procedimento_id, codigo_procedimento = result
                self.dimension_maps['procedimento'][codigo_procedimento] = procedimento_id
                inseridas += 1
            else:
                existentes +=1
        
        conn.commit()
        self.logger.info(f"📥 dim_procedimento: {inseridas} novas, {existentes} existentes")
        print(f"      ✅ Dimensão procedimento carregada com sucesso!")
    
    def load_cids(self, df: pd.DataFrame, conn) -> None:
        """Carrega dim_cid com dados únicos""" 
        
        cursor = conn.cursor()

        # Extrai dados unicos de cid
        colunas_cid = ['Código do CID', 'Descrição do CID']
        dim_cid = df[colunas_cid].drop_duplicates()

        
        # ✅ FILTRO MAIS ROBUSTO
        dim_cid = dim_cid[
            dim_cid['Código do CID'].notna() & 
            (dim_cid['Código do CID'] != '') &
            (dim_cid['Código do CID'].astype(str).str.strip() != '')
        ].copy()

        print(f"   ✅ Após filtro: {len(dim_cid)} registros válidos")

        # 3. Cria registro "CID Não Informado" para valores nulos
        cursor.execute("""
            INSERT INTO dim_cid (codigo_cid, descricao_cid)
            VALUES ('NI', 'CID Não Informado')
            ON CONFLICT (codigo_cid) DO NOTHING
            RETURNING cid_id, codigo_cid;
        """)

        result = cursor.fetchone()

        if result:
            cid_id, codigo_cid = result
            self.dimension_maps['cid'][codigo_cid] = cid_id
            print(f"      ✅ Registro 'CID Não Informado' criado: ID {cid_id}")
        else:
            # Se já existir, busca o ID existente
            cursor.execute("SELECT cid_id FROM dim_cid WHERE codigo_cid = 'NI'")
            result = cursor.fetchone()
            if result:
                self.dimension_maps['cid']['NI'] = result[0]

        # Contador para debug
        inseridas = 0
        existentes = 0

        for _, row in dim_cid.iterrows():
            # Garante que o código do CID é string
            codigo_cid = str(row['Código do CID'].strip())
            descricao_cid = row['Descrição do CID']

            cursor.execute("""
                           INSERT INTO dim_cid (codigo_cid, descricao_cid)
                            VALUES (%s, %s)
                            ON CONFLICT (codigo_cid) DO NOTHING
                           RETURNING cid_id, codigo_cid;""",
                           (codigo_cid, 
                            descricao_cid))
            
            result = cursor.fetchone()
            if result:
                cid_id, codigo_cid = result
                self.dimension_maps['cid'][codigo_cid] = cid_id
                inseridas += 1
            else:
                existentes +=1
        
        conn.commit()
        self.logger.info(f"📥 dim_cid: {inseridas} novas, {existentes} existentes")
        print(f"      ✅ Dimensão cid carregada com sucesso!")
    
    def load_cbos(self, df: pd.DataFrame, conn) -> None:
        """Carrega dim_cbo com dados únicos"""
        
        cursor = conn.cursor()

        # Extrai dados unicos de cbo
        colunas_cbo = ['Código do CBO', 'Descrição do CBO']
        dim_cbo = df[colunas_cbo].drop_duplicates()

        # Contador para debug
        inseridas = 0
        existentes = 0

        for _, row in dim_cbo.iterrows():
            cursor.execute("""
                           INSERT INTO dim_cbo (codigo_cbo, descricao_cbo)
                            VALUES (%s, %s)
                            ON CONFLICT (codigo_cbo) DO NOTHING
                           RETURNING cbo_id, codigo_cbo;""",
                           (row['Código do CBO'], 
                            row['Descrição do CBO']))
            
            result = cursor.fetchone()
            if result:
                cbo_id, codigo_cbo = result
                self.dimension_maps['cbo'][codigo_cbo] = cbo_id
                inseridas += 1
            else:
                existentes +=1
        
        conn.commit()
        self.logger.info(f"📥 dim_cbo: {inseridas} novas, {existentes} existentes")
        print(f"      ✅ Dimensão cbo carregada com sucesso!")
    
    def load_perfis(self, df: pd.DataFrame, conn) -> None:
        """Carrega dim_perfil_paciente com dados únicos"""
        
        cursor = conn.cursor()

        colunas_perfil = [
        'cod_usuario', 'Sexo', 'Data de Nascimento', 'Nacionalidade', 
        'origem_usuario', 'Município', 'Bairro',
        'Tratamento no Domicílio', 'Abastecimento', 'Energia Elétrica', 
        'Tipo de Habitação', 'Destino Lixo', 'Fezes/Urina', 'Cômodos', 
        'Em Caso de Doença', 'Grupo Comunitário', 'Meio de Comunicacao', 
        'Meio de Transporte'
        ]

        # Remove duplicatas e pega ultima ocorrencia
        dim_perfil = df[colunas_perfil].drop_duplicates(subset=['cod_usuario'], keep='last')

        # Contador para debug
        inseridas = 0
        existentes = 0
        total_linhas = len(dim_perfil)

        for index, (_, row) in enumerate(dim_perfil.iterrows()):
            
            # ✅ CONVERSÃO CRÍTICA: Garantir que cod_usuario seja INT
            try:
                cod_usuario_int = int(row['cod_usuario'])
            except (ValueError, TypeError):
                print(f"❌ Erro ao converter cod_usuario: {row['cod_usuario']}")
                continue

            # ✅ MOSTRAR PROGRESSO (a cada 5.000 linhas ou 10% do total)
            if index % 5000 == 0 and index > 0:
                percentual = (index / total_linhas) * 100
                print(f"         📈 Progresso carregamento dim_perfil: {index:,}/{total_linhas:,} ({percentual:.1f}%)")
            
            cursor.execute("""
            INSERT INTO dim_perfil_paciente (
                codigo_usuario, sexo, data_nascimento, nacionalidade,
                origem_usuario, municipio, bairro,
                tratamento_domicilio, abastecimento, energia_eletrica,
                tipo_habitacao, destino_lixo, fezes_urina, comodos,
                em_caso_doenca, grupo_comunitario, meio_comunicacao, 
                meio_transporte
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (codigo_usuario) 
            DO UPDATE SET
                sexo = EXCLUDED.sexo,
                data_nascimento = EXCLUDED.data_nascimento,
                nacionalidade = EXCLUDED.nacionalidade,
                origem_usuario = EXCLUDED.origem_usuario,
                municipio = EXCLUDED.municipio,
                bairro = EXCLUDED.bairro,
                tratamento_domicilio = EXCLUDED.tratamento_domicilio,
                abastecimento = EXCLUDED.abastecimento,
                energia_eletrica = EXCLUDED.energia_eletrica,
                tipo_habitacao = EXCLUDED.tipo_habitacao,
                destino_lixo = EXCLUDED.destino_lixo,
                fezes_urina = EXCLUDED.fezes_urina,
                comodos = EXCLUDED.comodos,
                em_caso_doenca = EXCLUDED.em_caso_doenca,
                grupo_comunitario = EXCLUDED.grupo_comunitario,
                meio_comunicacao = EXCLUDED.meio_comunicacao,
                meio_transporte = EXCLUDED.meio_transporte
            RETURNING perfil_id, codigo_usuario
            """, (
                cod_usuario_int, 
                row['Sexo'], 
                row['Data de Nascimento'],
                row['Nacionalidade'],
                row['origem_usuario'],
                row['Município'],
                row['Bairro'],
                row.get('Tratamento no Domicílio', None),
                row.get('Abastecimento', None),
                row['Energia Elétrica'],
                row.get('Tipo de Habitação', None),
                row.get('Destino Lixo', None),
                row.get('Fezes/Urina', None),
                row.get('Cômodos', None),
                row.get('Em Caso de Doença', None),
                row.get('Grupo Comunitário', None),
                row.get('Meio de Comunicacao', None),
                row.get('Meio de Transporte', None)
            ))

            result = cursor.fetchone()
            if result:
                perfil_id, codigo_usuario = result
                self.dimension_maps['perfil'][int(codigo_usuario)] = perfil_id
                
                # Verifica se foi INSERT ou UPDATE
                if cursor.statusmessage.startswith('INSERT'):
                    inseridas += 1
                else:
                    existentes += 1

        conn.commit()
        self.logger.info(f"📥 dim_perfil_paciente: {inseridas} novos, {existentes} atualizados")
        print(f"      ✅ Dimensão perfil carregada! {inseridas} novos, {existentes} atualizados")
    
