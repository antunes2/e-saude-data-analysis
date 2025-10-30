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