"""
AWS Lambda Worker - Procesa productos individuales desde SQS y los guarda en Supabase
Esta Lambda se ejecuta cuando hay mensajes en la cola SQS
"""

import json
import boto3
import os
import asyncio
import logging
from typing import Dict, Optional

# Importar scrapers y clientes
import sys
sys.path.append('/opt/python')  # Para Lambda layer
from rapsodia_scrapper import RapsodiaSafeScraper
from scrapers.supabase_client import SupabaseClient
from scrapers.mappers import get_rapsodia_category_mapping, normalize_price, normalize_sizes

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicializar cliente de Secrets Manager
secrets_client = boto3.client('secretsmanager')
SECRET_NAME = os.environ.get('SUPABASE_SECRET_NAME', 'supabase-credentials')


def get_supabase_connection_string() -> str:
    """
    Obtiene la string de conexión de Supabase desde AWS Secrets Manager
    
    Returns:
        String de conexión a Supabase
    """
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(response['SecretString'])
        return secret.get('database_url') or secret.get('connection_string')
    except Exception as e:
        logger.error(f"Error getting secret: {e}")
        # Fallback a variable de entorno
        return os.environ.get('SUPABASE_DB_URL', '')


async def process_product_url(url: str, brand: str, db_client: SupabaseClient) -> Dict:
    """
    Procesa una URL de producto individual
    
    Args:
        url: URL del producto
        brand: Nombre de la marca
        db_client: Cliente de Supabase
        
    Returns:
        Dict con el resultado del procesamiento
    """
    try:
        # Determinar scraper según la marca
        if brand.lower() == 'rapsodia':
            scraper = RapsodiaSafeScraper()
        else:
            # Agregar más scrapers aquí
            raise ValueError(f"Unknown brand: {brand}")
        
        # Extraer categoría y subcategoría de la URL (o del mensaje)
        # Por ahora, intentamos inferirlo de la URL
        category_name, subcategory_name = infer_category_from_url(url)
        
        # Extraer detalles del producto
        product_data = await scraper.extract_product_details(url, category_name, subcategory_name)
        
        if not product_data:
            return {
                'success': False,
                'error': 'Failed to extract product data',
                'url': url
            }
        
        # Normalizar datos (el scraper actual ya devuelve formato estándar)
        # Mapear campos del scraper actual al formato esperado
        normalized_data = {
            'name': product_data.get('nombre_producto') or product_data.get('title', ''),
            'description': product_data.get('description', ''),
            'price': product_data.get('precio_oferta') or product_data.get('precio_regular') or product_data.get('price', '0'),
            'sizes': product_data.get('talles_todos') or product_data.get('sizes', []),
            'available_sizes': product_data.get('talles_disponibles') or product_data.get('available_sizes', []),
            'availability': product_data.get('availability', True),
            'image_urls': product_data.get('image_urls', []),
            'colors': product_data.get('colors', [])
        }
        
        # Obtener IDs de marca y categorías
        brand_id = db_client.get_brand_id(brand)
        category_mapping = get_rapsodia_category_mapping(category_name, subcategory_name)
        
        if not category_mapping:
            logger.warning(f"Could not map category {category_name}/{subcategory_name} for {url}")
            category_mapping = {'category_id': None, 'subcategory_id': None}
        
        # Preparar datos para insertar
        item_data = {
            'name': normalized_data.get('name', ''),
            'description': normalized_data.get('description', ''),
            'price': normalize_price(normalized_data.get('price', '0')),
            'brand_id': brand_id,
            'category_id': category_mapping.get('category_id'),
            'subcategory_id': category_mapping.get('subcategory_id'),
            'sizes': normalize_sizes(normalized_data.get('sizes', [])),
            'sizes_available': normalize_sizes(normalized_data.get('available_sizes', [])),
            'available': normalized_data.get('availability', True)
        }
        
        # Upsert item
        item_id = db_client.upsert_item(item_data)
        
        if not item_id:
            return {
                'success': False,
                'error': 'Failed to upsert item',
                'url': url
            }
        
        # Procesar imágenes
        image_urls = normalized_data.get('image_urls', [])
        if image_urls:
            db_client.upsert_item_images(item_id, image_urls)
        
        # Procesar colores
        colors = normalized_data.get('colors', [])
        if colors:
            color_ids = []
            for color_name in colors:
                color_id = db_client.get_color_id(color_name)
                if color_id:
                    color_ids.append(color_id)
            
            if color_ids:
                db_client.link_item_colors(item_id, color_ids)
        
        return {
            'success': True,
            'item_id': item_id,
            'url': url
        }
        
    except Exception as e:
        logger.error(f"Error processing {url}: {e}")
        return {
            'success': False,
            'error': str(e),
            'url': url
        }


def infer_category_from_url(url: str) -> tuple:
    """
    Infiere la categoría y subcategoría desde la URL
    
    Args:
        url: URL del producto
        
    Returns:
        Tuple (category_name, subcategory_name)
    """
    # Lógica básica para inferir desde URL de Rapsodia
    if '/woman/' in url:
        category = 'WOMAN'
        if '/jean' in url:
            subcategory = 'JEANS'
        elif '/camisas-y-tops' in url or '/remeras' in url:
            subcategory = 'CAMISAS_Y_TOPS'
        elif '/vestidos' in url:
            subcategory = 'VESTIDOS'
        elif '/pantalones' in url:
            subcategory = 'PANTALONES'
        else:
            subcategory = 'GENERAL'
    elif '/girls/' in url:
        category = 'GIRLS'
        subcategory = 'GENERAL'
    else:
        category = 'UNKNOWN'
        subcategory = 'GENERAL'
    
    return (category, subcategory)


def lambda_handler(event, context):
    """
    Handler principal de la Lambda Worker
    
    Args:
        event: Evento de SQS con los mensajes
        context: Contexto de AWS Lambda
    """
    logger.info(f"Processing {len(event.get('Records', []))} messages")
    
    # Obtener conexión a Supabase
    conn_string = get_supabase_connection_string()
    db_client = SupabaseClient(conn_string)
    
    results = []
    
    # Procesar cada mensaje
    for record in event.get('Records', []):
        try:
            # Parsear mensaje
            message_body = json.loads(record['body'])
            url = message_body.get('url')
            brand = message_body.get('brand', 'Rapsodia')
            
            if not url:
                logger.warning("Message missing URL")
                continue
            
            # Procesar producto
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(
                process_product_url(url, brand, db_client)
            )
            results.append(result)
            
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            results.append({
                'success': False,
                'error': str(e)
            })
    
    # Retornar resultados
    successful = sum(1 for r in results if r.get('success'))
    failed = len(results) - successful
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': len(results),
            'successful': successful,
            'failed': failed,
            'results': results
        })
    }

