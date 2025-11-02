
# Análise de Dados de Saúde
=======
# e-saude-data-project

🎯 Objetivo
Pipeline ETL profissional para análise integrada de dados de saúde pública de Curitiba. Integra dados médicos, climáticos e geográficos em data warehouse relacional, permitindo análises correlacionais avançadas.

🔄 Fluxo de Dados
Extrai dados CSV brutos → Transforma e enriquece com Python/Pandas → Carrega para PostgreSQL com modelagem dimensional → Analisa via SQL, Jupyter e Power BI para insights estratégicos em saúde pública.

Dados extraidos de https://dadosabertos.curitiba.pr.gov.br/conjuntodado/detalhe?chave=05954644-5595-4dcb-b961-1e31e22a1c6e, https://portal.inmet.gov.br/ e OpenStreetMap

Stack Tecnológica

    Python 3.11+ (Pandas, Psycopg2, SQLAlchemy)

    PostgreSQL (Banco de dados dimensional)

    Power BI (Visualizações e dashboards)

    Git (Controle de versão)



📊 Modelagem Dimensional (Star Schema)
Tabelas Dimensão

    dim_unidade - Unidades de saúde

    dim_procedimento - Procedimentos médicos

    dim_cid - Classificação Internacional de Doenças

    dim_cbo - Classificação Brasileira de Ocupações

    dim_perfil_paciente - Perfis demográficos dos pacientes

Tabela Fato 

    fato_atendimento - Métricas e medidas dos atendimentos

        FKs: Todas as dimensões acima

        Medidas: Quantidades prescritas, idade, diferenças

        Controle: Chave natural para evitar duplicatas

🔄 Fluxo ETL Implementado
1. Extração (HealthETLPipeline.extract())

        Leitura de CSVs com dados de saúde

        Padronização de nomes de colunas

        Detecção automática de encoding

2. Transformação (HealthETLPipeline.transform())

        Conversão de datas (dd/mm/aaaa HH:MM:SS → datetime)

        Preservação de zeros à esquerda em códigos

        Cálculo de idades e faixas etárias

        Criação de chaves naturais únicas

        Tratamento de valores nulos e duplicatas

3. Carga (HealthETLPipeline.load())

        DimensionLoader: Carga incremental com ON CONFLICT

        FactLoader: Carregamento de medidas com lookup de FKs

        Tratamento robusto para valores missing (registro "NI" para CID não informado)