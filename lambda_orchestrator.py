"""
AWS Lambda Orchestrator - Descubre URLs de productos y las envía a SQS
Esta Lambda se ejecuta periódicamente (ej: diariamente) para encontrar nuevos productos
"""

import json
import boto3
import os
import asyncio
import logging
from typing import List

# Importar el scraper actual (temporal hasta refactorizar)
import sys
sys.path.append('/opt/python')  # Para Lambda layer
from rapsodia_scrapper import RapsodiaSafeScraper

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicializar cliente SQS
sqs = boto3.client('sqs')
QUEUE_URL = os.environ.get('SQS_QUEUE_URL', 'scraper-item-urls-queue')


async def discover_and_enqueue(category_urls: List[str], scraper: RapsodiaSafeScraper):
    """
    Descubre URLs de productos y las envía a SQS
    
    Args:
        category_urls: Lista de URLs de categorías a explorar
        scraper: Instancia del scraper
    """
    all_product_urls = []
    
    for category_url in category_urls:
        try:
            # Usar parse_subcategory_page para descubrir productos
            subcategory_data = await scraper.parse_subcategory_page(category_url, "WOMAN")
            product_urls = subcategory_data.get("products", [])
            all_product_urls.extend(product_urls)
            logger.info(f"Found {len(product_urls)} products in {category_url}")
            
            # Si hay paginación, procesar páginas siguientes (limitado para Lambda)
            next_page = subcategory_data.get("next_page")
            page_count = 1
            while next_page and page_count < 3:  # Limitar a 3 páginas para Lambda
                page_count += 1
                next_data = await scraper.parse_subcategory_page(next_page, "WOMAN")
                next_products = next_data.get("products", [])
                all_product_urls.extend(next_products)
                logger.info(f"Found {len(next_products)} products in page {page_count}")
                next_page = next_data.get("next_page")
                if not next_page:
                    break
                    
        except Exception as e:
            logger.error(f"Error discovering products in {category_url}: {e}")
    
    # Enviar URLs a SQS en lotes
    batch_size = 10  # SQS permite hasta 10 mensajes por batch
    for i in range(0, len(all_product_urls), batch_size):
        batch = all_product_urls[i:i + batch_size]
        
        entries = []
        for idx, url in enumerate(batch):
            entries.append({
                'Id': str(i + idx),
                'MessageBody': json.dumps({
                    'url': url,
                    'brand': 'Rapsodia',
                    'timestamp': asyncio.get_event_loop().time()
                })
            })
        
        try:
            response = sqs.send_message_batch(
                QueueUrl=QUEUE_URL,
                Entries=entries
            )
            logger.info(f"Sent batch of {len(batch)} URLs to SQS")
        except Exception as e:
            logger.error(f"Error sending batch to SQS: {e}")
    
    return len(all_product_urls)


def lambda_handler(event, context):
    """
    Handler principal de la Lambda Orchestrator
    
    Args:
        event: Evento de AWS Lambda (puede contener category_urls)
        context: Contexto de AWS Lambda
    """
    logger.info("Starting orchestrator lambda")
    
    # Obtener URLs de categorías del evento o usar defaults
    category_urls = event.get('category_urls', [
        'https://www.rapsodia.com.ar/woman/jean.html',
        'https://www.rapsodia.com.ar/woman/camisas-y-tops.html',
        # Agregar más categorías según necesites
    ])
    
    # Crear instancia del scraper actual
    scraper = RapsodiaSafeScraper()
    
    # Ejecutar descubrimiento
    loop = asyncio.get_event_loop()
    total_urls = loop.run_until_complete(
        discover_and_enqueue(category_urls, scraper)
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Discovered and enqueued {total_urls} product URLs',
            'total_urls': total_urls
        })
    }


# Deployment trigger Sun Nov 23 20:25:41 -03 2025
