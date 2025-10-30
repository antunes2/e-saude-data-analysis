-- sql/01_create_tables.sql
-- Script para criar o schema dimensional completo para o projeto e-saude

-- 1. DIMENSÕES PRINCIPAIS
CREATE TABLE dim_unidade (
    unidade_id SERIAL PRIMARY KEY,
    codigo_unidade VARCHAR(150) NOT NULL,
    descricao_unidade VARCHAR(80) NOT NULL,
    tipo_unidade VARCHAR(50) NOT NULL,
    area_atuacao VARCHAR(255),
    CONSTRAINT unique_codigo_unidade UNIQUE (codigo_unidade)
);

CREATE TABLE dim_procedimento (
    procedimento_id SERIAL PRIMARY KEY,
    codigo_procedimento VARCHAR(12) NOT NULL,
    descricao_procedimento VARCHAR(255) NOT NULL,
    CONSTRAINT unique_codigo_procedimento UNIQUE (codigo_procedimento)
);

CREATE TABLE dim_cid (
    cid_id SERIAL PRIMARY KEY,
    codigo_cid VARCHAR(4) NOT NULL,
    descricao_cid VARCHAR(150) NOT NULL,
    CONSTRAINT unique_codigo_cid UNIQUE (codigo_cid)
);

CREATE TABLE dim_cbo (
    cbo_id SERIAL PRIMARY KEY,
    codigo_cbo VARCHAR(8) NOT NULL,
    descricao_cbo VARCHAR(200) NOT NULL,
    CONSTRAINT unique_codigo_cbo UNIQUE (codigo_cbo)
);

-- 2. DIMENSÃO DE PERFIL DO PACIENTE (NOVA)
CREATE TABLE dim_perfil_paciente (
    perfil_id SERIAL PRIMARY KEY,
    -- Dados de perfil socioeconômico
    tratamento_domicilio VARCHAR(255),
    abastecimento VARCHAR(255),
    energia_eletrica VARCHAR(255),
    tipo_habitacao VARCHAR(255),
    destino_lixo VARCHAR(255),
    fezes_urina VARCHAR(255),
    comodos INTEGER,
    em_caso_doenca VARCHAR(255),
    grupo_comunitario VARCHAR(255),
    meio_comunicacao VARCHAR(255),
    meio_transporte VARCHAR(255),
    municipio VARCHAR(255),
    bairro VARCHAR(255),
    nacionalidade VARCHAR(255),
    -- Chaves naturais originais para vinculação
    cod_usuario INTEGER NOT NULL,
    origem_usuario INTEGER,
    residente INTEGER,
    cod_profissional INTEGER,
    -- Garante que cada usuário apareça apenas uma vez nesta dimensão
    CONSTRAINT unique_cod_usuario UNIQUE (cod_usuario)
);

-- 3. TABELA FATO PRINCIPAL
-- 🗑️ APAGAR a versão antiga
DROP TABLE IF EXISTS fato_atendimento;

-- ✅ RECRIAR com design Star Schema correto
CREATE TABLE fato_atendimento (
    -- CHAVE PRIMÁRIA
    atendimento_id SERIAL PRIMARY KEY,
    
    -- ✅ CHAVES ESTRANGEIRAS (apenas IDs das dimensões)
    unidade_id INTEGER NOT NULL REFERENCES dim_unidade(unidade_id),
    procedimento_id INTEGER NOT NULL REFERENCES dim_procedimento(procedimento_id),
    cid_id INTEGER NOT NULL REFERENCES dim_cid(cid_id),
    cbo_id INTEGER NOT NULL REFERENCES dim_cbo(cbo_id),
    perfil_id INTEGER NOT NULL REFERENCES dim_perfil_paciente(perfil_id),
    
    -- ✅ MEDIDAS/FATOS (métricas quantitativas)
    qtde_prescrita INTEGER,
    qtde_dispensada INTEGER,
    qtde_nao_padronizado INTEGER,
    
    -- ✅ MÉTRICAS CALCULADAS (do seu transform())
    idade_paciente INTEGER,
    diff_prescrito_dispensado INTEGER,
    gerou_internamento INTEGER, -- 0 ou 1
    
    -- ✅ ATRIBUTOS DE CONTEXTO (não são medidas, não são dimensões)
    data_atendimento TIMESTAMP NOT NULL,
    morador_curitiba_rm VARCHAR(20),
    periodo_dia VARCHAR(10),
    faixa_etaria VARCHAR(15),
    
    -- ✅ CONTROLE DE CARGA
    chave_natural VARCHAR(255) UNIQUE NOT NULL,
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para performance
CREATE INDEX idx_fato_data_atendimento ON fato_atendimento(data_atendimento);
CREATE INDEX idx_fato_unidade ON fato_atendimento(unidade_id);
CREATE INDEX idx_fato_perfil ON fato_atendimento(perfil_id);

-- 5. COMENTÁRIOS PARA DOCUMENTAÇÃO
COMMENT ON TABLE dim_perfil_paciente IS 'Armazena informações socioeconômicas dos pacientes. Uma linha por paciente.';
COMMENT ON TABLE fato_atendimento IS 'Armazena os eventos de atendimento. Uma linha por atendimento.';
COMMENT ON COLUMN fato_atendimento.chave_natural IS 'Chave natural composta para controle de carga incremental: data_atendimento + cod_unidade + cod_usuario + cod_procedimento';


 # ----------------------------------------------------

 -- RECRIAR TODAS AS TABELAS DIMENSÃO COM CONSTRAINTS UNIQUE

-- 1. DIM_UNIDADE
DROP TABLE IF EXISTS dim_unidade CASCADE;
CREATE TABLE dim_unidade (
    unidade_id SERIAL PRIMARY KEY,
    codigo_unidade VARCHAR(50) NOT NULL UNIQUE,
    descricao_unidade VARCHAR(255),
    codigo_tipo_unidade VARCHAR(10),
    tipo_unidade VARCHAR(100)
);

-- 2. DIM_PROCEDIMENTO
DROP TABLE IF EXISTS dim_procedimento CASCADE;
CREATE TABLE dim_procedimento (
    procedimento_id SERIAL PRIMARY KEY,
    codigo_procedimento VARCHAR(50) NOT NULL UNIQUE,
    descricao_procedimento VARCHAR(255)
);

-- 3. DIM_CID
DROP TABLE IF EXISTS dim_cid CASCADE;
CREATE TABLE dim_cid (
    cid_id SERIAL PRIMARY KEY,
    codigo_cid VARCHAR(20) NOT NULL UNIQUE,
    descricao_cid VARCHAR(255)
);

-- 4. DIM_CBO
DROP TABLE IF EXISTS dim_cbo CASCADE;
CREATE TABLE dim_cbo (
    cbo_id SERIAL PRIMARY KEY,
    codigo_cbo VARCHAR(20) NOT NULL UNIQUE,
    descricao_cbo VARCHAR(255)
);

-- 5. DIM_PERFIL_PACIENTE (já corrigida anteriormente)
DROP TABLE IF EXISTS dim_perfil_paciente CASCADE;
CREATE TABLE dim_perfil_paciente (
    perfil_id SERIAL PRIMARY KEY,
    sexo VARCHAR(10),
    data_nascimento DATE,
    municipio VARCHAR(255),
    bairro VARCHAR(255),
    nacionalidade VARCHAR(255),
    tratamento_domicilio VARCHAR(255),
    abastecimento VARCHAR(255),
    energia_eletrica VARCHAR(255),
    tipo_habitacao VARCHAR(255),
    destino_lixo VARCHAR(255),
    fezes_urina VARCHAR(255),
    comodos INTEGER,
    em_caso_doenca VARCHAR(255),
    grupo_comunitario VARCHAR(255),
    meio_comunicacao VARCHAR(255),
    meio_transporte VARCHAR(255),
    codigo_usuario INTEGER NOT NULL UNIQUE,
    origem_usuario INTEGER
);

#===========================================================

-- 1. Limpar dados
TRUNCATE TABLE fato_atendimento CASCADE;
TRUNCATE TABLE dim_perfil_paciente CASCADE;
TRUNCATE TABLE dim_cbo CASCADE;
TRUNCATE TABLE dim_cid CASCADE;
TRUNCATE TABLE dim_procedimento CASCADE;
TRUNCATE TABLE dim_unidade CASCADE;

-- 2. Resetar sequences (CRÍTICO!)
ALTER SEQUENCE dim_perfil_paciente_perfil_id_seq RESTART WITH 1;
ALTER SEQUENCE dim_unidade_unidade_id_seq RESTART WITH 1;
ALTER SEQUENCE dim_procedimento_procedimento_id_seq RESTART WITH 1;
ALTER SEQUENCE dim_cid_cid_id_seq RESTART WITH 1;
ALTER SEQUENCE dim_cbo_cbo_id_seq RESTART WITH 1;
ALTER SEQUENCE fato_atendimento_atendimento_id_seq RESTART WITH 1;

-- 3. Verificar sequences (COMANDO CORRETO)
SELECT sequencename, start_value, last_value
FROM pg_sequences 
WHERE sequencename LIKE '%dim_%' OR sequencename LIKE '%fato_%';

=====================================

-- Adicionar colunas de coordenadas na dim_unidade
ALTER TABLE dim_unidade 
ADD COLUMN IF NOT EXISTS latitude DECIMAL(10, 8),
ADD COLUMN IF NOT EXISTS longitude DECIMAL(10, 8),
ADD COLUMN IF NOT EXISTS endereco_completo TEXT,
ADD COLUMN IF NOT EXISTS data_geocoding TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Criar índice para performance em mapas
CREATE INDEX IF NOT EXISTS idx_unidade_coordinates 
ON dim_unidade (latitude, longitude);