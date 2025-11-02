from scripts.etl_pipeline import HealthETLPipeline
from scripts.climate_pipeline import ClimateETLPipeline
from scripts.geocoding.geocoding_helper import run_geocoding_pipeline
import time
import logging
import sys
import os  # ← IMPORTANTE: Adicione este import

def setup_logging():
    """Configura logging para todo o sistema"""
    
    # ✅ CORREÇÃO: Criar pasta logs se não existir
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        print(f"📁 Pasta de logs criada: {log_dir}")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/pipeline_execution.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def get_user_choice(prompt, options):
    """Menu interativo para o usuário"""
    print(f"\n{prompt}")
    for key, value in options.items():
        print(f"  {key}. {value}")
    
    while True:
        choice = input("\nEscolha uma opção: ").strip()
        if choice in options:
            return choice
        print("❌ Opção inválida. Tente novamente.")

def run_health_pipeline():
    """Executa pipeline de saúde com tratamento de erro"""
    try:
        print("\n" + "="*60)
        print("🏥 INICIANDO PIPELINE DE SAÚDE")
        print("="*60)
        
        health_pipeline = HealthETLPipeline()
        health_pipeline.run()
        
        print("✅ Pipeline de saúde concluído com sucesso!")
        return True
        
    except Exception as e:
        print(f"❌ Erro no pipeline de saúde: {e}")
        logging.error(f"Health pipeline failed: {e}")
        return False

def run_climate_pipeline():
    """Executa pipeline climático com tratamento de erro"""
    try:
        print("\n" + "="*60)
        print("🌤️  INICIANDO PIPELINE CLIMÁTICO")
        print("="*60)
        
        climate_pipeline = ClimateETLPipeline()
        climate_pipeline.run()
        
        print("✅ Pipeline climático concluído com sucesso!")
        return True
        
    except Exception as e:
        print(f"❌ Erro no pipeline climático: {e}")
        logging.error(f"Climate pipeline failed: {e}")
        return False

def run_climate_optional():
    """Menu opcional para pipeline climático"""
    options = {
        '1': 'Executar pipeline climático',
        '2': 'Pular pipeline climático'
    }
    
    choice = get_user_choice(
        "🌤️  DESEJA EXECUTAR PIPELINE CLIMÁTICO?",
        options
    )
    
    if choice == '1':
        return run_climate_pipeline()
    else:
        print("⏭️  Pipeline climático pulado.")
        return None  # None indica que foi pulado intencionalmente

def run_geocoding_optional():
    """Menu opcional para geocoding"""
    options = {
        '1': 'Executar geocoding para TODAS as unidades',
        '2': 'Executar geocoding para TESTE (apenas 5 unidades)',
        '3': 'Pular geocoding'
    }
    
    choice = get_user_choice(
        "🗺️  DESEJA EXECUTAR GEOCODING DAS UNIDADES?",
        options
    )
    
    if choice == '1':
        return run_geocoding_pipeline()
    elif choice == '2':
        return run_geocoding_pipeline(max_units=5)
    else:
        print("⏭️  Geocoding pulado.")
        return None  # None indica que foi pulado intencionalmente

def show_final_stats(start_time, results):
    """Mostra estatísticas finais da execução"""
    total_time = time.time() - start_time
    
    print("\n" + "="*60)
    print("📊 RELATÓRIO FINAL DE EXECUÇÃO")
    print("="*60)
    
    print(f"⏱️  Tempo total: {total_time:.2f} segundos")
    
    # Saúde (sempre executado)
    health_status = "✅" if results['health'] else "❌"
    print(f"🏥 Pipeline saúde: {health_status}")
    
    # Climático (opcional)
    climate_status = "✅" if results['climate'] is True else "❌" if results['climate'] is False else "⏭️"
    print(f"🌤️  Pipeline climático: {climate_status}")
    
    # Geocoding (opcional)
    geocoding_status = "✅" if results['geocoding'] is True else "❌" if results['geocoding'] is False else "⏭️"
    print(f"🗺️  Geocoding: {geocoding_status}")
    
    # Resumo
    successful = sum(1 for r in results.values() if r is True)
    total_executed = sum(1 for r in results.values() if r is not None)
    
    if successful == total_executed:
        print(f"\n🎉 TODOS OS PIPELINES EXECUTADOS CONCLUÍDOS COM SUCESSO!")
    elif successful > 0:
        print(f"\n⚠️  {successful}/{total_executed} pipelines executados com sucesso")
    else:
        print(f"\n💥 Todos os pipelines executados falharam")

def main():
    """Função principal do sistema E-Saúde Curitiba"""
    start_time = time.time()
    setup_logging()
    
    print("🚀 SISTEMA E-SAÚDE CURITIBA - INICIANDO")
    print("📍 Análise integrada de dados de saúde pública")
    
    # Resultados de cada pipeline
    # True = sucesso, False = falha, None = pulado intencionalmente
    execution_results = {
        'health': False,      # Saúde é obrigatório
        'climate': None,      # Climático é opcional
        'geocoding': None     # Geocoding é opcional
    }
    
    try:
        # 1. Pipeline Principal - Saúde (SEMPRE executa)
        execution_results['health'] = run_health_pipeline()
        
        # 2. Pipeline Climático (OPCIONAL - independente do saúde)
        execution_results['climate'] = run_climate_optional()
        
        # 3. Geocoding (OPCIONAL - só precisa do saúde para existir)
        if execution_results['health']:
            execution_results['geocoding'] = run_geocoding_optional()
        else:
            print("⚠️  Geocoding pulado - precisa do pipeline de saúde para ter unidades no banco")
            execution_results['geocoding'] = None
        
        # 4. Estatísticas Finais
        show_final_stats(start_time, execution_results)
        
    except KeyboardInterrupt:
        print(f"\n⏹️  Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n💥 Erro crítico no sistema: {e}")
        logging.critical(f"System failure: {e}")
    finally:
        print(f"\n👋 Finalizando Sistema E-Saúde Curitiba")

if __name__ == "__main__":
    main()