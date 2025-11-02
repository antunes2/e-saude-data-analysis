import pandas as pd
from geopy.geocoders import Nominatim
import time
import psycopg2
from src.config.database import DatabaseConfig
import re
import logging

class GeoCodingHelper:
    """
    Classe para geocoding de unidades de saúde usando OpenStreetMap
    Foco em nomes COMPLETOS e descritivos para melhor busca
    """
    
    def __init__(self):
        self.geolocator = Nominatim(user_agent="curitiba_health_project_v1")
        self.logger = logging.getLogger(__name__)
        
    def clean_unit_name(self, unit_name):
        """
        Padroniza nomes mantendo TERMOS COMPLETOS para melhor geocoding
        OpenStreetMap responde melhor a nomes descritivos
        """
        if not isinstance(unit_name, str):
            return unit_name
        
        # Converter para minúsculas e remover espaços extras
        cleaned = unit_name.lower().strip()
        
        # 🔧 SUBSTITUIÇÕES ESTRATÉGICAS - MANTENDO TERMOS COMPLETOS
        replacements = {
            'upa': 'unidade de pronto atendimento',
            'ums': 'unidade de saúde',
            'us': 'unidade de saúde', 
            'psf': '',  # Remove PSF - não ajuda na busca
            'ubs': 'unidade básica de saúde',
            'ciaf': '',  # Remove CIAF - não ajuda
            ' / ': ' ',
            '  ': ' ',
            # NÃO converter "unidade de saúde" para "us" - prejudica a busca!
        }
        
        for old, new in replacements.items():
            cleaned = cleaned.replace(old, new)
        
        # 🎯 PADRÕES PARA NOMES MAIS LEGÍVEIS
        patterns = [
            (r'us\s+(\w+)', r'unidade de saúde \1'),  # "us x" → "unidade de saúde x"
            (r'ums\s+(\w+)', r'unidade de saúde \1'), # "ums x" → "unidade de saúde x" 
            (r'upa\s+(\w+)', r'unidade de pronto atendimento \1'), # "upa x" → mantém
        ]
        
        for pattern, replacement in patterns:
            cleaned = re.sub(pattern, replacement, cleaned)
        
        # Remove múltiplos espaços e capitaliza para melhor legibilidade
        cleaned = ' '.join(cleaned.split())
        cleaned = cleaned.title()  # "Unidade De Saúde Santa Felicidade"
        
        return cleaned
    
    def is_in_curitiba(self, location):
        """Verifica se a localização está em Curitiba"""
        if not location or not location.address:
            return False
        address = location.address.lower()
        return 'curitiba' in address or 'paraná' in address

    def smart_geocoding(self, unit_name):
        """
        Estratégias de busca com NOMES COMPLETOS para melhor precisão
        """
        strategies = [
            # 🥇 ESTRATÉGIA 1: Nome limpo COMPLETO + Curitiba (MAIS EFETIVA)
            {
                'query': f"{self.clean_unit_name(unit_name)}, Curitiba, PR, Brazil",
                'description': 'Nome completo padronizado'
            },
            
            # 🥈 ESTRATÉGIA 2: Nome limpo + "unidade de saúde" + bairro (se detectável)
            {
                'query': f"unidade de saúde {self._extract_main_name(unit_name)}, Curitiba, PR, Brazil",
                'description': 'Busca genérica por unidade de saúde'
            },
            
            # 🥉 ESTRATÉGIA 3: Nome original (fallback)
            {
                'query': f"{unit_name}, Curitiba, PR, Brazil", 
                'description': 'Nome original'
            },
        ]
        
        for strategy in strategies:
            try:
                query = strategy['query']
                if not query or 'None' in query or query == ', Curitiba, PR, Brazil':
                    continue
                    
                self.logger.info(f"    [{strategy['description']}]")
                self.logger.info(f"      Buscando: {query}")
                
                location = self.geolocator.geocode(query)
                
                if location and self.is_in_curitiba(location):
                    self.logger.info(f"   Encontrado via: {strategy['description']}")
                    self.logger.info(f"      Endereço: {location.address}")
                    return {
                        'latitude': location.latitude,
                        'longitude': location.longitude,
                        'endereco_completo': location.address,
                        'query_utilizada': query,
                        'estrategia': strategy['description']
                    }
                
                time.sleep(1.2)  # Respeitar rate limit
                    
            except Exception as e:
                self.logger.warning(f"   Erro na estratégia {strategy['description']}: {e}")
                continue
        
        return None

    def _extract_main_name(self, unit_name):
        """
        Tenta extrair a parte principal do nome (provavelmente o bairro/local)
        de forma SEGURA, sem assumir que tudo é bairro
        """
        # Remove siglas comuns
        cleaned = unit_name.lower()
        words_to_remove = ['upa', 'ums', 'us', 'psf', 'ubs', 'ciaf', 'unidade', 'de', 'saúde', 'pronto', 'atendimento', 'básica']
        
        for word in words_to_remove:
            cleaned = cleaned.replace(word, '')
        
        # Pega a primeira palavra significativa restante
        words = [word.strip().title() for word in cleaned.split() if word.strip() and len(word.strip()) > 2]
        
        if words:
            return words[0]  # Retorna apenas a primeira palavra significativa
        return ""

    def get_units_from_database(self):
        """Busca unidades do banco de dados que precisam de coordenadas"""
        try:
            with DatabaseConfig.get_connection() as conn:
                cursor = conn.cursor()
            
                cursor.execute("""
                    SELECT unidade_id, codigo_unidade, descricao_unidade 
                    FROM dim_unidade 
                    WHERE latitude IS NULL OR longitude IS NULL
                    ORDER BY descricao_unidade
                """)
                
                units = cursor.fetchall()

                return [{'id': u[0], 'codigo': u[1], 'nome': u[2]} for u in units]
                
        except Exception as e:
            self.logger.error(f" Erro ao buscar unidades do banco: {e}")
            return []



    def update_unit_coordinates(self, unit_id, latitude, longitude, endereco):
        """Atualiza coordenadas no banco de dados"""
        try:
            with DatabaseConfig.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    UPDATE dim_unidade 
                    SET latitude = %s, longitude = %s, endereco_completo = %s,
                        data_geocoding = CURRENT_TIMESTAMP
                    WHERE unidade_id = %s
                """, (latitude, longitude, endereco, unit_id))
                
                conn.commit()
                return True
            
        except Exception as e:
            self.logger.error(f"Erro ao atualizar unidade {unit_id}: {e}")
            return False

    def _save_reports(self, results, not_found):
        """Salva relatórios detalhados de sucesso e falha"""
        try:
            if results:
                df_success = pd.DataFrame(results)
                df_success.to_csv('data/processed/unidades_coordenadas_sucesso.csv', 
                                index=False, encoding='utf-8')
                print(f"   💾 Relatório de sucesso salvo: {len(results)} unidades")
            
            if not_found:
                df_failure = pd.DataFrame(not_found)
                df_failure.to_csv('data/processed/unidades_coordenadas_falha.csv', 
                                index=False, encoding='utf-8')
                print(f"   💾 Relatório de falha salvo: {len(not_found)} unidades")
                
        except Exception as e:
            self.logger.error(f" Erro ao salvar relatórios: {e}")

    def _show_summary(self, results, not_found):
        """Mostra resumo e instruções para unidades não encontradas"""
        print(f"\n🎯 RESULTADO FINAL:")
        print(f"   ✅ Processadas com sucesso: {len(results)}")
        print(f"   ❌ Não encontradas/Falha: {len(not_found)}")
        
        if not_found:
            print(f"\n📋 PRIMEIRAS 10 UNIDADES NÃO ENCONTRADAS:")
            for unit in not_found[:10]:
                print(f"   • {unit['nome']} - {unit.get('erro', 'Erro desconhecido')}")
            
            if len(not_found) > 10:
                print(f"   ... e mais {len(not_found) - 10} unidades")
            
            print(f"\n💡 AÇÕES RECOMENDADAS:")
            print(f"   1. Verifique o arquivo: data/processed/unidades_coordenadas_falha.csv")
            print(f"   2. Busque coordenadas MANUALMENTE no Google Maps para essas unidades")
            print(f"   3. Atualize diretamente no banco com:")
            print(f"      UPDATE dim_unidade SET latitude=XX, longitude=YY WHERE descricao_unidade='NOME';")
        
        return len(not_found) == 0

    def process_all_units(self, max_units=None):
        """Processa todas as unidades do banco que precisam de coordenadas"""
        
        units = self.get_units_from_database()
        
        if not units:
            self.logger.info(" Todas as unidades já possuem coordenadas!")
            return True
        
        # Limitar unidades se especificado (útil para testes)
        if max_units:
            units = units[:max_units]
        
        self.logger.info(f"📍 Encontradas {len(units)} unidades para processar")
        
        results = []
        not_found = []
        
        for i, unit in enumerate(units, 1):
            self.logger.info(f"\n [{i}/{len(units)}] Processando: {unit['nome']}")
            
            # 🎯 DEBUG: Mostrar como o nome será transformado
            nome_limpo = self.clean_unit_name(unit['nome'])
            self.logger.info(f"    Nome para busca: {nome_limpo}")
            
            coordinates = self.smart_geocoding(unit['nome'])
            
            if coordinates:
                # Atualizar no banco
                success = self.update_unit_coordinates(
                    unit['id'],
                    coordinates['latitude'],
                    coordinates['longitude'], 
                    coordinates['endereco_completo']
                )
                
                if success:
                    results.append({
                        'unidade_id': unit['id'],
                        'nome_original': unit['nome'],
                        'nome_busca': nome_limpo,
                        **coordinates
                    })
                    self.logger.info(f"    Coordenadas salvas!")
                else:
                    not_found.append({**unit, 'erro': 'Falha ao salvar no banco'})
                    self.logger.error(f"    Erro ao salvar no banco")
            else:
                not_found.append({**unit, 'erro': 'Coordenadas não encontradas no OpenStreetMap'})
                self.logger.warning(f"    Coordenadas não encontradas")
        
        # 💾 Salvar relatório detalhado
        self._save_reports(results, not_found)
        
        return self._show_summary(results, not_found)

# ✅ CORREÇÃO CRÍTICA: Função separada para importação
def run_geocoding_pipeline(max_units=None):
    """
    Função para executar o pipeline de geocoding
    Pode ser chamada do main.py ou executada separadamente
    """
    print("🗺️  INICIANDO PROCESSO DE GEOCODING...")
    print("📍 Estratégia: Nomes COMPLETOS + OpenStreetMap")
    
    geocoder = GeoCodingHelper()
    success = geocoder.process_all_units(max_units)
    
    if success:
        print("✅ Processo de geocoding concluído com sucesso!")
    else:
        print("⚠️  Processo concluído com algumas falhas - verifique os relatórios")
    
    return success

# ✅ CORREÇÃO: Código de execução standalone SEPARADO
def main_standalone():
    """Função para execução DIRECTA do arquivo apenas"""
    print("🚀 EXECUÇÃO STANDALONE DO GEOCODING HELPER")
    run_geocoding_pipeline()

# ✅ CORREÇÃO: Este código só roda quando o arquivo é executado DIRETAMENTE
if __name__ == "__main__":
    main_standalone()