"""
Mappers - Mapea categorías y datos de diferentes scrapers a Supabase
"""

from typing import Dict, Optional, Tuple

# Mapeo de categorías principales de Rapsodia a Supabase
RAPSODIA_CATEGORY_MAP = {
    'WOMAN': {
        'category_id': '124af253-3a60-417c-88d9-47bfe3835479',  # Prendas
        'category_slug': 'prendas',
        'subcategories': {
            'JEANS': {
                'id': '0885711c-7e8c-4bdb-a924-6d3cd48cb269',
                'slug': 'jeans'
            },
            'PANTALONES': {
                'id': 'b27e6165-8c44-4f86-b5b0-564f05049f29',
                'slug': 'pantalones'
            },
            'REMERAS': {
                'id': '7095e13c-470f-4352-a8f0-542119a42c09',  # Tops
                'slug': 'tops'
            },
            'CAMISAS_Y_TOPS': {
                'id': '7095e13c-470f-4352-a8f0-542119a42c09',  # Tops
                'slug': 'tops'
            },
            'VESTIDOS': {
                'id': '4e2d0317-dbc8-44a8-be44-22111b5c3921',
                'slug': 'vestidos'
            },
            'BUZOS_SWEATERS': {
                'id': '93488394-8772-4ae5-82b4-1dc898118fba',  # Abrigos
                'slug': 'abrigos'
            },
            'CAMPERAS_KIMONOS': {
                'id': '93488394-8772-4ae5-82b4-1dc898118fba',  # Abrigos
                'slug': 'abrigos'
            },
        }
    },
    'GIRLS': {
        'category_id': '124af253-3a60-417c-88d9-47bfe3835479',  # Prendas
        'category_slug': 'prendas',
        'subcategories': {
            'JEANS': {
                'id': '0885711c-7e8c-4bdb-a924-6d3cd48cb269',
                'slug': 'jeans'
            },
            'PANTALONES': {
                'id': 'b27e6165-8c44-4f86-b5b0-564f05049f29',
                'slug': 'pantalones'
            },
            'REMERAS_CAMISAS': {
                'id': '7095e13c-470f-4352-a8f0-542119a42c09',  # Tops
                'slug': 'tops'
            },
            'BUZOS_SWEATERS': {
                'id': '93488394-8772-4ae5-82b4-1dc898118fba',  # Abrigos
                'slug': 'abrigos'
            },
            'CAMPERAS_SACOS': {
                'id': '93488394-8772-4ae5-82b4-1dc898118fba',  # Abrigos
                'slug': 'abrigos'
            },
        }
    }
}


def get_rapsodia_category_mapping(category_name: str, subcategory_name: str) -> Optional[Dict]:
    """
    Obtiene el mapeo de categorías de Rapsodia a Supabase
    
    Args:
        category_name: Nombre de la categoría (WOMAN, GIRLS, etc.)
        subcategory_name: Nombre de la subcategoría
        
    Returns:
        Dict con category_id y subcategory_id, o None si no se encuentra
    """
    category_info = RAPSODIA_CATEGORY_MAP.get(category_name)
    if not category_info:
        return None
    
    subcategory_info = category_info['subcategories'].get(subcategory_name)
    if not subcategory_info:
        return None
    
    return {
        'category_id': category_info['category_id'],
        'category_slug': category_info['category_slug'],
        'subcategory_id': subcategory_info['id'],
        'subcategory_slug': subcategory_info['slug']
    }


def normalize_price(price_text: str) -> float:
    """
    Normaliza el texto de precio a un número
    
    Args:
        price_text: Texto del precio (ej: "$ 240.000", "$240,000")
        
    Returns:
        Precio como float
    """
    if not price_text:
        return 0.0
    
    # Remover símbolos y espacios
    price_clean = price_text.replace('$', '').replace(',', '').replace('.', '').strip()
    
    try:
        return float(price_clean)
    except ValueError:
        return 0.0


def normalize_sizes(sizes: list) -> list:
    """
    Normaliza la lista de talles
    
    Args:
        sizes: Lista de talles (puede contener strings, números, etc.)
        
    Returns:
        Lista normalizada de talles como strings
    """
    if not sizes:
        return []
    
    normalized = []
    for size in sizes:
        if size:
            # Convertir a string y limpiar
            size_str = str(size).strip().upper()
            if size_str and size_str not in normalized:
                normalized.append(size_str)
    
    return normalized

