"""
Base Scraper Class - Clase base abstracta para todos los scrapers
Permite crear scrapers modulares y extensibles para diferentes sitios web
"""

from abc import ABC, abstractmethod
from typing import List, Set, Dict, Optional
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from bs4 import BeautifulSoup
import logging
import asyncio
from collections import defaultdict

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Clase base abstracta para todos los scrapers.
    Define la interfaz común que todos los scrapers deben implementar.
    """
    
    def __init__(self, base_url: str, brand_name: str):
        """
        Inicializa el scraper base
        
        Args:
            base_url: URL base del sitio web a scrapear
            brand_name: Nombre de la marca (ej: "Rapsodia", "Kosiuko")
        """
        self.base_url = base_url
        self.brand_name = brand_name
        self.discovered_urls = set()
        self.product_urls = set()
        self.product_urls_extracted = set()
        self.category_urls = set()
        
        # Configuración de timeouts y reintentos
        self.timeout_seconds = 30
        self.max_retries = 3
        self.retry_delay = 2
        
        # Contadores de errores
        self.error_counts = defaultdict(int)
        self.timeout_counts = defaultdict(int)
        self.successful_requests = 0
        self.failed_requests = 0
        
        # Configuración del navegador
        self.browser_config = BrowserConfig(
            headless=True,
            extra_args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor"
            ]
        )
        
        self.crawl_config = CrawlerRunConfig(
            markdown_generator=DefaultMarkdownGenerator()
        )
    
    @abstractmethod
    async def discover_product_urls(self, category_url: str) -> List[str]:
        """
        Descubre todas las URLs de productos en una categoría
        
        Args:
            category_url: URL de la categoría a explorar
            
        Returns:
            Lista de URLs de productos encontradas
        """
        pass
    
    @abstractmethod
    async def extract_product_details(self, url: str, category_name: str, subcategory_name: str) -> Optional[Dict]:
        """
        Extrae los detalles de un producto individual
        
        Args:
            url: URL del producto
            category_name: Nombre de la categoría
            subcategory_name: Nombre de la subcategoría
            
        Returns:
            Dict con los detalles del producto o None si hay error
        """
        pass
    
    @abstractmethod
    def normalize_product_data(self, product_data: Dict) -> Dict:
        """
        Normaliza los datos del producto al formato estándar
        
        Args:
            product_data: Datos del producto en formato del scraper específico
            
        Returns:
            Datos normalizados al formato estándar
        """
        pass
    
    async def safe_crawl_page(self, url: str, context: str = "unknown") -> Optional[Dict]:
        """
        Método seguro para hacer crawling de una página con manejo de errores
        
        Args:
            url: URL a hacer crawling
            context: Contexto para logging
            
        Returns:
            Dict con 'success', 'html', 'error' o None si falla
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                async with AsyncWebCrawler(config=self.browser_config) as crawler:
                    result = await crawler.arun(
                        url=url,
                        config=self.crawl_config
                    )
                    
                    if result.success and result.html:
                        self.successful_requests += 1
                        return {
                            'success': True,
                            'html': result.html,
                            'markdown': result.markdown if hasattr(result, 'markdown') else None
                        }
                    else:
                        error_msg = f"Failed to crawl {url}: No HTML content"
                        logger.warning(f"{error_msg} (attempt {attempt}/{self.max_retries})")
                        
            except Exception as e:
                error_msg = f"Error crawling {url}: {str(e)}"
                logger.error(f"{error_msg} (attempt {attempt}/{self.max_retries})")
                self.error_counts[context] += 1
                
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay * attempt)
                else:
                    self.failed_requests += 1
                    return {
                        'success': False,
                        'error': error_msg
                    }
        
        return {
            'success': False,
            'error': f"Failed after {self.max_retries} attempts"
        }
    
    def validate_url(self, url: str) -> bool:
        """Valida que una URL sea válida"""
        if not url or not isinstance(url, str):
            return False
        return url.startswith('http://') or url.startswith('https://')
    
    def is_product_url(self, url: str) -> bool:
        """
        Determina si una URL es de un producto individual
        Debe ser implementado por cada scraper específico
        """
        return False

