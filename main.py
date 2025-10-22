from scripts.etl_pipeline import HealthETLPipeline
from scripts.climate_pipeline import ClimateETLPipeline
import time

def main():
    print("🚀 Iniciando Sistema E-Saúde Curitiba")
    start_time = time.time()
    
    # 1. Pipeline de Saúde (PRINCIPAL)
    print("\n" + "="*50)
    health_pipeline = HealthETLPipeline()
    health_pipeline.run()
    
    # 2. Pipeline Climático (COMPLEMENTAR)
    print("\n" + "="*50)
    climate_pipeline = ClimateETLPipeline()
    climate_pipeline.run()
    
    # 3. Estatísticas Finais
    print("\n" + "="*50)
    total_time = time.time() - start_time
    print(f"🎉 Todos os pipelines concluídos em {total_time:.2f} segundos!")
    print("💡 Próximo passo: Análise integrada saúde-clima")

if __name__ == "__main__":
    main()