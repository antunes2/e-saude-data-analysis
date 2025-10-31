import pandas as pd
from geopy.geocoders import Nominatim
import time
import psycopg2
from src.config.database import DatabaseConfig
import re
import logging

class GeoCodingHelper:
    """
    Classe auxiliar para geocodificação de endereços usando OpenStreetMap.
    """
    
    def __init__(self):
        self.geolocator = Nominatim(user_agent="curitiba_health_project_v1")
        self.logger = logging.getLogger(__name__)

    def clean_unit_name(self, unit_name):
        """Padroniza nomes das unidades para melhor geocoding"""
        if not isinstance(unit_name, str):
            return unit_name
        
        # Converte para minúsculas e remova caracteres especiais
        cleaned = unit_name.lower().strip()

        # Substituicoes para que o mapa reconheca as unidades
        replacements = {
            'upa': 'unidade de pronto atendimento',
            'ums': 'unidade de saúde',
            'us': 'unidade de saúde',
            'psf': '',
            'ubs': 'unidade básica de saúde',
            'ciaf': 'centro integrado de atenção à família',
            ' / ': ' ',
            '  ': ' '
        }

        for old, new in replacements.items():
            cleaned = cleaned.replace(old, new)

        # PADRÕES ESPECÍFICOS COM REGEX
        patterns = [
            (r'us\s+(\w+)', r'unidade de saúde \1'),
            (r'ums\s+(\w+)', r'unidade de saúde \1'),
            (r'upa\s+(\w+)', r'unidade de pronto atendimento \1'),
        ]
        
        for pattern, replacement in patterns:
            cleaned = re.sub(pattern, replacement, cleaned)

        # Remover multiplos espaços e capitalizar

        cleaned = ' '.join(cleaned.split())
        cleaned = ' '.join(word.capitalize() for word in cleaned.split())

        return cleaned
    
    def is_in_curitiba(self, location):
        """Verifica se a localização está em Curitiba"""
        if not location or not location.address:
            return False
        address = location.address.lower()
        return 'curitiba' in address and 'paraná' in address
    
    def smart_geocoding(self, unit_name):
        """
        busca com nomes completos para aumentar a precisao
        """

        strategies = [
            # Estratégia 1: Nome da unidade limpo + Curitiba 
            {
                'query': f"{self.clean_unit_name(unit_name)}, Curitiba, PR, Brasil",
                'description': 'Nome completo padronizado'
            },

            # ESTRATÉGIA 2: Nome limpo + "unidade de saúde" + bairro (se detectável)
            {
                'query': f"unidade de saúde {self._extract_main_name(unit_name)}, Curitiba, PR, Brazil",
                'description': 'Busca genérica por unidade de saúde'
            },
            
            # ESTRATÉGIA 3: Nome original (fallback)
            {
                'query': f"{unit_name}, Curitiba, PR, Brazil", 
                'description': 'Nome original'
            },
        ]

        for strategy in strategies:

            try:
                query = strategy['query']
                if not query or None in query or query == ', Curitiba, PR, Brazil':
                    continue

                self.logger.info(f"   🔍 [{strategy['description']}]")
                self.logger.info(f"      Buscando: {query}")

                location = self.geolocator.geocode(query, timeout=10)

                if location and self.is_in_curitiba(location):
                    self.logger.info(f"   ✅ Encontrado via: {strategy['description']}")
                    self.logger.info(f"      Endereço: {location.address}")

                    return {
                        'latitude': location.latitude,
                        'longitude': location.longitude,
                        'endereco_completo': location.address,
                        'query_usada': query,
                        'metodo': strategy['description']
                    }
                
                time.sleep(1.2)  # Evita sobrecarga no Nominatim

            except Exception as e:
                self.logger.error(f"Erro na geocodificação com {strategy['description']}: {e}")
                continue

            return None

    def _extract_main_name(self, unit_name):
        """Extrai o nome principal da unidade, removendo siglas e termos genéricos"""
        
        # Remover siglas comuns
        cleaned = unit_name
        words_to_remove = ['upa', 'ums', 'us', 'psf', 'ubs', 'ciaf', 'unidade', 'de', 'saúde', 'pronto', 'atendimento', 'básica']

        for word in words_to_remove:
            cleaned = cleaned.replace(word, '')

        # Pega a primeira palavra significativa restante
        words = [word.strip().title() for word in cleaned.split() if word.strip() and len(word.strip()) > 2]    
        
        if words:
            return words[0] # Retorna a primeira palavra significativa
        return ""
    
    def get_units_from_db(self):
        """Busca unidades de saúde do banco de dados"""
        query = "SELECT cod_unidade, nome_unidade FROM dim_unidade WHERE latitude IS NULL OR longitude IS NULL;"
        
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
            SELECT unidade_id, codigo_unidade, nome_unidade 
            FROM dim_unidade 
            WHERE latitude IS NULL OR longitude IS NULL
            ORDER BY nome_unidade
        """)
            
            units = cursor.fetchall()
            cursor.close()
            conn.close()
            
            return [{'id': u[0], 'codigo': u[1], 'nome': u[2]} for u in units]

        except Exception as e:
            self.logger.error(f"Erro ao buscar unidades do DB: {e}")
            return []              
        
    def update_unit_coordinates(self, unit_id, latitude, longitude, endereco):
        """Atualiza coordenadas da unidade no banco de dados"""
        try:
            conn = DatabaseConfig.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE dim_unidade
                SET latitude = %s, longitude = %s, endereco_completo = %s,
                data_geocoding = CURRENT_TIMESTAMP
                WHERE unidade_id = %s;
            """, (latitude, longitude, endereco, unit_id))
            
            conn.commit()
            cursor.close()
            conn.close()

            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao atualizar unidade {unit_id}: {e}")

    def _safe_reports(self, results, not_found):
        """Salva relatórios detalhados de sucesso e falha"""
        try:
            if results:
                df_success = pd.DataFrame(results)
                df_success.to_csv('data/processed/unidades_coordenadas_sucesso.csv', 
                            index=False, encoding='utf-8')
                print(f"   💾 Relatório de sucesso salvo: {len(results)} unidades")
            
            if not_found:
                df_fail = pd.DataFrame(not_found)
                df_fail.to_csv('data/processed/unidades_coordenadas_falha.csv', 
                            index=False, encoding='utf-8')
                print(f"   💾 Relatório de falhas salvo: {len(not_found)} unidades")

        except Exception as e:
            self.logger.error(f"Erro ao salvar relatórios: {e}")

    def _show_summary(self, results, not_found):
        """Mostra resumo dos resultados de geocoding"""
        print("\n📊 Resumo da Geocodificação:")
        print(f"   ✅ Sucesso: {len(results)} unidades")
        print(f"   ❌ Falha: {len(not_found)} unidades")

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
                print(f"      UPDATE dim_unidade SET latitude=XX, longitude=YY WHERE nome_unidade='NOME';")
        
        return len(not_found) == 0
    
    def process_all_units(self, max_units = None):
        """Processa todas as unidades do banco que precisam de coordenadas"""

        units = self.get_units_from_db()

        if not units:
            self.logger.info("✅ Todas as unidades já possuem coordenadas!")
            return True

        # Limitar para testes rápidos
        if max_units:
            units = units[:max_units]
        
        self.logger.info(f"📍 Encontradas {len(units)} unidades para processar")

        results = []
        not_found = []

        for i, unit in enumerate(units, start=1):
            self.logger.info(f"\n🔹 [{i}/{len(units)}] Processando: {unit['nome']}")
            
            # DEBUG: mostrar como o nome será transformado
            nome_limpo = self.clean_unit_name(unit['nome'])
            self.logger.info(f"      Nome limpo: {nome_limpo}")

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
                    self.logger.info(f"   ✅ Coordenadas salvas!")
                else:
                    not_found.append({**unit, 'erro': 'Falha ao salvar no banco'})
                    self.logger.error(f"   ❌ Erro ao salvar no banco")
            else:
                not_found.append({**unit, 'erro': 'Coordenadas não encontradas no OpenStreetMap'})
                self.logger.error(f"   ❌ Coordenadas não encontradas")

        # 💾 Salvar relatório detalhado
        self._save_reports(results, not_found)
        
        return self._show_summary(results, not_found)  
    
    def run_geocoding_pipeline(max_units=None):
        """
        Função para executar o pipeline de geocoding
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

    # def main_standalone():
    #     """Função principal para execução STANDALONE apenas"""
    #     print("🚀 EXECUÇÃO STANDALONE DO GEOCODING")
    #     run_geocoding_pipeline()

    # if __name__ == "__main__":
    #     # Quando executado diretamente, roda como standalone
    #     main_standalone()