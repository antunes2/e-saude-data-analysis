from scripts.etl_pipeline import HealthETLPipeline

def test_step_by_step():
    """Testa cada etapa do pipeline separadamente"""
    pipeline = HealthETLPipeline()
    
    print("🧪 TESTE INCREMENTAL DO PIPELINE")
    print("=" * 50)
    
    # Teste 1: Apenas extração
    print("\n1. Testando EXTRACT...")
    pipeline.extract()
    print(f"   ✅ DataFrame shape: {pipeline.df.shape}")
    
    # Teste 2: Extração + transformação de datas
    print("\n2. Testando TRANSFORM (datas)...")
    pipeline._convert_dates()
    print(f"   ✅ Tipos de data: {pipeline.df['Data do Atendimento'].dtype}")
    
    # Salva amostra para inspeção
    sample = pipeline.df.head(100)
    sample.to_csv('data/processed/transformacao_teste.csv', index=False)
    print("   💾 Amostra salva para inspeção")

if __name__ == "__main__":
    test_step_by_step()