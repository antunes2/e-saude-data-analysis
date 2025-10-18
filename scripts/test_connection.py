# test_connection.py
import psycopg2
from psycopg2 import sql

# 1. Conecta ao banco
try:
    conn = psycopg2.connect(
        host="localhost",
        database="eSaudeCuritiba",  # Nome do seu BD
        user="postgres",
        password="2042609hg"       # ⚠️ Coloque sua senha real aqui!
    )
    print("✅ Conexão com PostgreSQL bem-sucedida!")
except Exception as e:
    print(f"❌ Erro de conexão: {e}")
    exit()

# 2. Cria um cursor para executar comandos
cursor = conn.cursor()

# 3. Tenta inserir um registro de teste na DIM_UNIDADE
try:
    insert_query = """
    INSERT INTO dim_unidade (codigo_unidade, descricao_unidade, tipo_unidade, area_atuacao)
    VALUES (%s, %s, %s, %s)
    RETURNING unidade_id;  -- Retorna o ID gerado
    """
    
    valores = ('TESTE_001', 'Unidade de Teste', 'UPA', 'Área Teste')
    
    cursor.execute(insert_query, valores)
    id_gerado = cursor.fetchone()[0]  # Pega o ID retornado
    print(f"✅ Insert bem-sucedido! ID gerado: {id_gerado}")
    
    # 4. Faz commit para salvar no banco
    conn.commit()
    
except Exception as e:
    print(f"❌ Erro no INSERT: {e}")
    conn.rollback()  # Desfaz a transação em caso de erro

# 5. Agora lê para confirmar que está lá
try:
    select_query = "SELECT * FROM dim_unidade WHERE codigo_unidade = 'TESTE_001';"
    cursor.execute(select_query)
    registro = cursor.fetchone()
    print(f"✅ Confirmação de leitura: {registro}")
    
except Exception as e:
    print(f"❌ Erro no SELECT: {e}")

# 6. Limpa o teste (opcional)
try:
    delete_query = "DELETE FROM dim_unidade WHERE codigo_unidade = 'TESTE_001';"
    cursor.execute(delete_query)
    conn.commit()
    print("✅ Registro de teste removido.")
    
except Exception as e:
    print(f"❌ Erro no DELETE: {e}")

# 7. Fecha a conexão
cursor.close()
conn.close()
print("✅ Conexão fechada. Teste concluído!")