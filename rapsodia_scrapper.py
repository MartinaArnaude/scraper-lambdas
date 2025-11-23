import asyncio
import signal
import sys
from typing import List, Set, Dict, Optional
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
import json
import time
import hashlib
from collections import defaultdict
import logging

# Configurar logging para debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rapsodia_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RapsodiaSafeScraper:
    def __init__(self):
        self.base_url = "https://www.rapsodia.com.ar"
        self.discovered_urls = set()
        self.product_urls = set()
        self.product_urls_extracted = set()  # Nuevo: URLs de productos ya procesados
        self.category_urls = set()
        self.session_id = "rapsodia_session"
        self.interrupted = False

        # Configuración de timeouts y reintentos
        self.timeout_seconds = 30
        self.max_retries = 3
        self.retry_delay = 2
        
        # Contadores de errores para monitoreo
        self.error_counts = defaultdict(int)
        self.timeout_counts = defaultdict(int)
        self.successful_requests = 0
        self.failed_requests = 0
        
        # Estructura organizada para categorías y productos
        self.category_structure = defaultdict(lambda: {
            'subcategories': set(),
            'products': set(),
            'url': ''
        })
        
        # Productos organizados por categoría
        self.products_by_category = defaultdict(list)
        
        # Tracking de progreso por categoría
        self.category_progress = {}
        self.visited_urls_per_category = defaultdict(set)
        self.new_products_found = defaultdict(int)
        self.consecutive_empty_pages = defaultdict(int)
        
        # Configuración del navegador con timeouts
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
        
        # Configurar manejo de interrupciones
        signal.signal(signal.SIGINT, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Maneja la interrupción Ctrl+C de forma segura"""
        print("\n\n⚠️  Interrupción detectada. Guardando datos recolectados...")
        self.interrupted = True
        self.save_all_data()
        print("✓ Datos guardados exitosamente. Saliendo...")
        sys.exit(0)
    
    async def safe_crawl_page(self, url: str, context: str = "unknown") -> Optional[Dict]:
        """
        Método seguro para hacer crawling de una página con manejo de errores y timeouts
        """
        # Validar URL antes de procesar
        if not self.validate_url(url):
            logger.error(f"Invalid URL for crawling: {url}")
            return {
                'success': False,
                'error': f'Invalid URL: {url}',
                'url': url
            }
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Attempting to crawl {url} (attempt {attempt + 1}/{self.max_retries})")
                
                # Usar asyncio.wait_for para timeout
                async with AsyncWebCrawler(config=self.browser_config) as crawler:
                    result = await asyncio.wait_for(
                        crawler.arun(
                            url=url,
                            config=self.crawl_config,
                            session_id=self.session_id
                        ),
                        timeout=self.timeout_seconds
                    )
                
                if result.success:
                    self.successful_requests += 1
                    logger.info(f"Successfully crawled {url}")
                    return {
                        'success': True,
                        'html': result.html,
                        'url': url
                    }
                else:
                    self.failed_requests += 1
                    error_msg = f"Failed to crawl {url}: {result.error_message}"
                    logger.warning(error_msg)
                    self.error_counts[context] += 1
                    
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.retry_delay * (attempt + 1))
                        continue
                    else:
                        return {
                            'success': False,
                            'error': result.error_message,
                            'url': url
                        }
                        
            except asyncio.TimeoutError:
                self.timeout_counts[context] += 1
                self.failed_requests += 1
                logger.warning(f"Timeout crawling {url} (attempt {attempt + 1})")
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                    continue
                else:
                    return {
                        'success': False,
                        'error': 'Timeout',
                        'url': url
                    }
                    
            except Exception as e:
                self.failed_requests += 1
                self.error_counts[context] += 1
                logger.error(f"Unexpected error crawling {url}: {e}")
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                    continue
                else:
                    return {
                        'success': False,
                        'error': str(e),
                        'url': url
                    }
        
        return {
            'success': False,
            'error': 'Max retries exceeded',
            'url': url
        }
    
    def normalize_url(self, href: str, base_url: str = None) -> str:
        """
        Normaliza URLs relativas a absolutas
        """
        if not href:
            return ""
        
        if base_url is None:
            base_url = self.base_url
        
        # Si ya es una URL absoluta, retornarla
        if href.startswith('http'):
            return href
        
        # Normalizar URL relativa
        try:
            normalized_url = urljoin(base_url, href)
            # Validar que la URL resultante sea válida
            if not normalized_url.startswith('http'):
                logger.warning(f"Invalid normalized URL: {normalized_url} from {href}")
                return ""
            return normalized_url
        except Exception as e:
            logger.warning(f"Error normalizing URL {href}: {e}")
            return ""
    
    def validate_url(self, url: str) -> bool:
        """
        Valida que una URL sea válida antes de procesarla
        """
        if not url:
            return False
        
        # Verificar que empiece con un protocolo válido
        if not url.startswith(('http://', 'https://', 'file://', 'raw:')):
            return False
        
        # Verificar que sea una URL válida de Rapsodia
        if 'rapsodia.com.ar' not in url:
            return False
        
        return True
    
    def create_next_page_url(self, current_url: str, page_number: int) -> str:
        """
        Crea una URL válida para la siguiente página
        """
        if not self.validate_url(current_url):
            logger.warning(f"Invalid current URL for pagination: {current_url}")
            return ""
        
        # Si la URL ya tiene parámetros, agregar el parámetro de página
        if '?' in current_url:
            separator = '&'
        else:
            separator = '?'
        
        return f"{current_url}{separator}p={page_number}"
    
    def validate_product_data(self, product_info: Dict) -> bool:
        """
        Valida que los datos del producto sean válidos antes de guardarlos
        """
        if not product_info:
            return False
        
        # Verificar que tenga URL
        if not product_info.get('url'):
            logger.warning("Product missing URL")
            return False
        
        # Verificar que la URL sea válida
        if not product_info['url'].startswith('http'):
            logger.warning(f"Invalid product URL: {product_info['url']}")
            return False
        
        # Verificar que tenga al menos título o descripción
        if not product_info.get('title') and not product_info.get('description'):
            logger.warning(f"Product missing title and description: {product_info['url']}")
            return False
        
        # Verificar que la descripción no sea el mensaje de JavaScript deshabilitado
        description = product_info.get('description', '')
        if description and ('JavaScript' in description or 'javascript' in description):
            logger.warning(f"Product has JavaScript disabled message: {product_info['url']}")
            # No invalidar el producto por esto, solo loggear
        
        return True
    
    def validate_category_has_products(self, category_name: str) -> bool:
        """
        Valida que una categoría tenga al menos un producto antes de guardar
        """
        products = self.products_by_category.get(category_name, [])
        if not products:
            logger.warning(f"Category {category_name} has no products")
            return False
        
        # Verificar que al menos un producto sea válido
        valid_products = [p for p in products if self.validate_product_data(p)]
        if not valid_products:
            logger.warning(f"Category {category_name} has no valid products")
            return False
        
        return True
    
    def is_product_url(self, url: str) -> bool:
        """
        Verifica si una URL es realmente de un producto individual
        """
        # Patrones que indican que NO es un producto
        non_product_patterns = [
            r'\.html\?',  # URLs con parámetros de filtro
            r'cat=',       # URLs de categorías
            r'talle_rap=', # URLs de talles
            r'color_filtro_cc=', # URLs de colores
            r'discount_rate=', # URLs de descuentos
            r'p=\d+',      # URLs de paginación
            r'faq/',       # Páginas de FAQ
            r'stores/',    # Páginas de tiendas
            r'como-comprar/', # Páginas de ayuda
            r'contact/',   # Páginas de contacto
            r'vintage/girls', # Subcategorías específicas que no queremos
            r'/woman/.*\.html$',  # URLs de categorías WOMAN
            r'/girls/.*\.html$',  # URLs de categorías GIRLS
            r'/denim/.*\.html$',  # URLs de categorías DENIM
            r'/sale/.*\.html$',   # URLs de categorías SALE
            r'/home/.*\.html$',   # URLs de categorías HOME
            r'/vintage/.*\.html$', # URLs de categorías VINTAGE
        ]
        
        # Si coincide con algún patrón de no-producto, retornar False
        for pattern in non_product_patterns:
            if re.search(pattern, url):
                return False
        
        # Patrones que indican que SÍ es un producto individual
        product_patterns = [
            r'^\d{8,}',  # URLs que empiezan con números largos (códigos de producto)
            r'producto/', # URLs con "producto" en la ruta
            r'product/',  # URLs con "product" en la ruta
            r'\d{8,}p\d+', # Patrón específico de Rapsodia: números + p + números
            r'\d{8,}.*\.html$',  # URLs con números largos que terminan en .html
        ]
        
        # Si coincide con algún patrón de producto, retornar True
        for pattern in product_patterns:
            if re.search(pattern, url):
                return True
        
        # Si no coincide con ningún patrón, ser más conservador
        return False
    
    async def validate_page_has_products(self, url: str) -> bool:
        """
        Valida si una página realmente contiene productos
        """
        try:
            # Usar el método seguro de crawling
            result = await self.safe_crawl_page(url, "page_validation")
            
            if not result['success']:
                logger.warning(f"Failed to validate page {url}: {result['error']}")
                return False
            
            soup = BeautifulSoup(result['html'], 'html.parser') if result['html'] else None
            
            if not soup:
                return False
            
            # Buscar productos usando la estructura específica de Rapsodia
            product_grid = soup.select_one("ol.products.list.items.product-items")
            if product_grid:
                product_items = product_grid.select("li.item.product.product-item")
                if len(product_items) > 0:
                    print(f"  ✓ Page validated: {len(product_items)} products found")
                    return True
            
            # Si no encontramos productos con la estructura específica, usar selectores genéricos
            product_selectors = [
                "a[href*='/producto/']", "a[href*='/product/']",
                ".product-item a", ".item a", ".product-link",
                "a[data-product]", "a[href*='.html']",
                ".product-card a", ".item-card a",
                "li.product-item a", "a.product-item-link", 
                "a[href*='p01']", "a[href*='p02']", "a[href*='p03']",
                ".product-item-photo-link", "a[href*='.html']"
            ]
            
            for selector in product_selectors:
                links = soup.select(selector)
                if links:
                    print(f"  ✓ Page validated: {len(links)} product links found")
                    return True
            
            print(f"  ✗ Page has no products: {url}")
            return False
            
        except Exception as e:
            logger.error(f"Error validating page {url}: {e}")
            return False
    
    def is_category_complete(self, category_name: str) -> bool:
        """
        Determina si una categoría está completa basándose en varios criterios
        """
        # Si no hemos encontrado productos en las últimas 3 páginas consecutivas
        if self.consecutive_empty_pages[category_name] >= 3:
            print(f"  📊 Categoría {category_name} completada: 3 páginas consecutivas sin productos")
            return True
        
        # Si hemos procesado más de 10 páginas sin encontrar nuevos productos
        if self.new_products_found[category_name] == 0 and len(self.visited_urls_per_category[category_name]) > 10:
            print(f"  📊 Categoría {category_name} completada: 10+ páginas sin nuevos productos")
            return True
        
        # Si hemos encontrado más de 200 productos en esta categoría
        if len(self.products_by_category[category_name]) > 200:
            print(f"  📊 Categoría {category_name} completada: 200+ productos encontrados")
            return True
        
        # Si hemos procesado más de 20 páginas en total
        if len(self.visited_urls_per_category[category_name]) > 20:
            print(f"  📊 Categoría {category_name} completada: 20+ páginas procesadas")
            return True
        
        return False
    
    def get_category_name_from_url(self, url: str) -> str:
        """
        Extrae el nombre de la categoría de la URL
        """
        if '/woman/' in url:
            return 'WOMAN'
        elif '/girls/' in url:
            return 'GIRLS'
        elif '/denim/' in url:
            return 'DENIM'
        elif '/sale/' in url:
            return 'SALE'
        elif '/home/' in url:
            return 'HOME'
        elif '/vintage/' in url:
            return 'VINTAGE'
        else:
            # Extraer de la URL
            path = urlparse(url).path.strip('/')
            if path:
                return path.upper().replace('-', '_').replace('/', '_')
            return 'UNKNOWN'
    
    def get_subcategory_name_from_url(self, url: str) -> str:
        """
        Extrae el nombre de la subcategoría de la URL para WOMAN, GIRLS y DENIM
        """
        # WOMAN subcategorías
        if '/woman/jean.html' in url or '/woman/jean/' in url:
            return 'JEANS'
        elif '/woman/camisas-y-tops.html' in url or '/woman/camisas-y-tops/' in url:
            return 'CAMISAS_Y_TOPS'
        elif '/woman/remeras.html' in url or '/woman/remeras/' in url:
            return 'REMERAS'
        elif '/woman/vestidos.html' in url or '/woman/vestidos/' in url:
            return 'VESTIDOS'
        elif '/woman/camperas-kimonos.html' in url or '/woman/camperas-kimonos/' in url:
            return 'CAMPERAS_KIMONOS'
        elif '/woman/pantalones.html' in url or '/woman/pantalones/' in url:
            return 'PANTALONES'
        elif '/woman/buzos-y-sweaters.html' in url or '/woman/buzos-y-sweaters/' in url:
            return 'BUZOS_SWEATERS'
        elif '/woman/polleras-y-shorts.html' in url or '/woman/polleras-y-shorts/' in url:
            return 'POLLERAS_SHORTS'
        elif '/woman/cueros.html' in url or '/woman/cueros/' in url:
            return 'CUEROS'
        elif '/woman/calzado-mujer.html' in url or '/woman/calzado-mujer/' in url:
            return 'CALZADO'
        elif '/woman/carteras-y-bolsos.html' in url or '/woman/carteras-y-bolsos/' in url:
            return 'CARTERAS_BOLSOS'
        elif '/woman/pa-uelos-y-accesorios.html' in url or '/woman/pa-uelos-y-accesorios/' in url:
            return 'PAÑUELOS_ACCESORIOS'
        elif '/woman/bijou.html' in url or '/woman/bijou/' in url:
            return 'BIJOU'
        elif '/woman/perfumes.html' in url or '/woman/perfumes/' in url:
            return 'PERFUMES'
        
        # GIRLS subcategorías
        elif '/girls/jeans.html' in url or '/girls/jeans/' in url:
            return 'JEANS'
        elif '/girls/remeras-y-camisas.html' in url or '/girls/remeras-y-camisas/' in url:
            return 'REMERAS_CAMISAS'
        elif '/girls/camperas-y-sacos.html' in url or '/girls/camperas-y-sacos/' in url:
            return 'CAMPERAS_SACOS'
        elif '/girls/pantalones.html' in url or '/girls/pantalones/' in url:
            return 'PANTALONES'
        elif '/girls/buzos-y-sweaters.html' in url or '/girls/buzos-y-sweaters/' in url:
            return 'BUZOS_SWEATERS'
        elif '/girls/calzado-nina.html' in url or '/girls/calzado-nina/' in url:
            return 'CALZADO'
        
        # DENIM subcategorías
        elif '/denim/jeans.html' in url or '/denim/jeans/' in url:
            return 'JEANS'
        elif '/denim/camperas.html' in url or '/denim/camperas/' in url:
            return 'CAMPERAS'
        elif '/denim/faldas.html' in url or '/denim/faldas/' in url:
            return 'FALDAS'
        elif '/denim/chalecos.html' in url or '/denim/chalecos/' in url:
            return 'CHALECOS'
        
        else:
            return 'GENERAL'
    
    def validate_next_page_url(self, url: str, current_url: str) -> bool:
        """
        Valida si una URL de paginación es válida y diferente a la actual
        """
        if not url:
            return False
        
        # Verificar que no sea la misma URL actual
        if url == current_url:
            return False
        
        # Verificar que sea una URL válida de Rapsodia
        if 'rapsodia.com.ar' not in url:
            return False
        
        # Verificar que no sea una URL de producto
        if self.is_product_url(url):
            return False
        
        # Verificar que no sea una URL de filtro o parámetros innecesarios
        invalid_patterns = [
            r'#',  # Anclas
            r'javascript:',  # JavaScript
            r'void\(0\)',  # JavaScript void
            r'cat=',  # Filtros de categoría
            r'talle_rap=',  # Filtros de talle
            r'color_filtro_cc=',  # Filtros de color
            r'discount_rate=',  # Filtros de descuento
        ]
        
        for pattern in invalid_patterns:
            if re.search(pattern, url):
                return False
        
        return True
    
    def extract_pagination_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """
        Extrae todos los enlaces de paginación válidos de una página
        """
        pagination_links = []
        
        # Selectores más específicos para Rapsodia
        pagination_selectors = [
            # Selectores específicos de paginación
            ".pagination a", ".pagination-item a", ".page-item a",
            "a[aria-label*='next']", "a[aria-label*='siguiente']",
            "a[title*='next']", "a[title*='siguiente']",
            "a.next", "a.next-page", "a[class*='next']",
            "a[href*='page=']", "a[href*='pagina=']", "a[href*='p=']",
            # Selectores genéricos de paginación
            ".pager a", ".pagination-container a", "nav.pagination a",
            # Selectores específicos para Rapsodia
            "a[data-page]", "a[data-pagination]", ".pagination-wrapper a",
            # Botones de "cargar más" o "ver más"
            "button[data-action*='load']", "a[data-action*='load']",
            ".load-more", ".show-more", ".view-more"
        ]
        
        for selector in pagination_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href:
                    full_url = self.normalize_url(href, self.base_url)
                    if full_url and self.validate_next_page_url(full_url, current_url):
                        pagination_links.append(full_url)
        
        # También buscar enlaces que contengan números de página
        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link.get('href')
            if href and re.search(r'p=\d+', href):  # Patrón de página
                full_url = self.normalize_url(href, self.base_url)
                if full_url and self.validate_next_page_url(full_url, current_url):
                    pagination_links.append(full_url)
        
        # Eliminar duplicados manteniendo el orden
        unique_links = []
        for link in pagination_links:
            if link not in unique_links:
                unique_links.append(link)
        
        return unique_links
    
    def find_next_page_url(self, pagination_links: List[str], current_url: str) -> Optional[str]:
        """
        Encuentra la URL de la siguiente página basándose en la URL actual
        """
        if not pagination_links:
            return None
        
        # Extraer el número de página actual si existe
        current_page_match = re.search(r'p=(\d+)', current_url)
        current_page = int(current_page_match.group(1)) if current_page_match else 1
        
        # Buscar la siguiente página
        for link in pagination_links:
            # Verificar si es la página siguiente
            page_match = re.search(r'p=(\d+)', link)
            if page_match:
                page_num = int(page_match.group(1))
                if page_num == current_page + 1:
                    return link
        
        # Si no encontramos una página específica, tomar el primer enlace válido
        # que no sea la página actual
        for link in pagination_links:
            if link != current_url:
                return link
        
        return None

    async def parse_category_page(self, url: str) -> Dict:
        """
        Parsea una página de categoría y extrae enlaces a subcategorías y productos
        """
        print(f"Parsing category page: {url}")
        
        try:
            # Usar el método seguro de crawling
            result = await self.safe_crawl_page(url, "category_page")
            
            if not result['success']:
                logger.error(f"Failed to crawl category page {url}: {result['error']}")
                return {"subcategories": [], "products": [], "next_page": None}
            
            # Extraer enlaces usando BeautifulSoup
            soup = BeautifulSoup(result['html'], 'html.parser') if result['html'] else None
            
            subcategories = []
            products = []
            next_page = None
            
            if soup:
                # Detectar paginación JavaScript
                js_pagination = self.detect_javascript_pagination(soup)
                if js_pagination['has_load_more'] or js_pagination['has_infinite_scroll']:
                    print(f"  🔍 JavaScript pagination detected: {js_pagination}")
                
                # Buscar enlaces de subcategorías
                subcategory_selectors = [
                    "a[href*='/woman/']", "a[href*='/girls/']", "a[href*='/denim/']",
                    "a[href*='/sale/']", "a[href*='/home/']", "a[href*='/vintage/']",
                    ".category-link", ".subcategory-link", "nav a"
                ]
                
                for selector in subcategory_selectors:
                    links = soup.select(selector)
                    for link in links:
                        href = link.get('href')
                        if href:
                            # Normalizar URL
                            full_url = self.normalize_url(href, url)
                            if full_url and full_url not in self.discovered_urls:
                                # Filtrar URLs que no son productos
                                if not self.is_product_url(full_url):
                                    subcategories.append(full_url)
                                    self.discovered_urls.add(full_url)
                
                # Buscar productos usando la estructura específica de Rapsodia
                # Primero buscar el contenedor de productos
                product_grid = soup.select_one("ol.products.list.items.product-items")
                if product_grid:
                    # Buscar todos los items de producto dentro del grid
                    product_items = product_grid.select("li.item.product.product-item")
                    print(f"  Found product grid with {len(product_items)} items")
                    
                    for item in product_items:
                        # Buscar enlaces de producto dentro de cada item
                        product_links = item.select("a[href*='.html']")
                        for link in product_links:
                            href = link.get('href')
                            if href:
                                # Normalizar URL
                                full_url = self.normalize_url(href, url)
                                if full_url and full_url not in self.product_urls:
                                    # Solo agregar si es realmente un producto
                                    if self.is_product_url(full_url):
                                        products.append(full_url)
                                        self.product_urls.add(full_url)
                
                # Si no encontramos productos con la estructura específica, usar selectores genéricos
                if not products:
                    print("  No products found in grid, trying generic selectors...")
                    product_selectors = [
                        "a[href*='/producto/']", "a[href*='/product/']",
                        ".product-item a", ".item a", ".product-link",
                        "a[data-product]", "a[href*='.html']",
                        ".product-card a", ".item-card a",
                        # Selectores específicos para Rapsodia
                        "li.product-item a", "a.product-item-link", 
                        "a[href*='p01']", "a[href*='p02']", "a[href*='p03']",
                        ".product-item-photo-link", "a[href*='.html']"
                    ]
                    
                    for selector in product_selectors:
                        links = soup.select(selector)
                        for link in links:
                            href = link.get('href')
                            if href:
                                # Normalizar URL
                                full_url = self.normalize_url(href, url)
                                if full_url and full_url not in self.product_urls:
                                    # Solo agregar si es realmente un producto
                                    if self.is_product_url(full_url):
                                        products.append(full_url)
                                        self.product_urls.add(full_url)
                
                # Buscar paginación usando el nuevo método
                pagination_links = self.extract_pagination_links(soup, url)
                next_page = self.find_next_page_url(pagination_links, url)
                
                # Si no encontramos paginación tradicional pero hay JavaScript pagination
                if not next_page and (js_pagination['has_load_more'] or js_pagination['has_infinite_scroll']):
                    print(f"  🔍 Using JavaScript pagination detection")
                    # Para JavaScript pagination, podríamos necesitar simular clicks o scroll
                    # Por ahora, marcamos que hay más contenido disponible
                    next_page = "javascript_pagination_detected"
                
                if next_page:
                    print(f"  Found next page: {next_page}")
                else:
                    print(f"  No next page found (total pagination links: {len(pagination_links)})")
            
            print(f"  Found {len(subcategories)} subcategories, {len(products)} products")
            return {
                "subcategories": subcategories,
                "products": products,
                "next_page": next_page
            }
            
        except Exception as e:
            logger.error(f"Error parsing category page {url}: {e}")
            return {"subcategories": [], "products": [], "next_page": None}

    async def parse_subcategory_page(self, url: str, category_name: str) -> Dict:
        """
        Parsea una página de subcategoría y extrae enlaces a productos
        """
        print(f"Parsing subcategory page: {url}")
        
        # Marcar URL como visitada para esta categoría
        self.visited_urls_per_category[category_name].add(url)
        
        try:
            # Usar el método seguro de crawling
            result = await self.safe_crawl_page(url, "subcategory_page")
            
            if not result['success']:
                logger.error(f"Failed to crawl subcategory page {url}: {result['error']}")
                return {"products": [], "next_page": None}
            
            soup = BeautifulSoup(result['html'], 'html.parser') if result['html'] else None
            
            products = []
            next_page = None
            new_products_in_page = 0
            
            if soup:
                # Detectar paginación JavaScript
                js_pagination = self.detect_javascript_pagination(soup)
                if js_pagination['has_load_more'] or js_pagination['has_infinite_scroll']:
                    print(f"  🔍 JavaScript pagination detected: {js_pagination}")
                
                # Buscar productos usando la estructura específica de Rapsodia
                # Primero buscar el contenedor de productos
                product_grid = soup.select_one("ol.products.list.items.product-items")
                if product_grid:
                    # Buscar todos los items de producto dentro del grid
                    product_items = product_grid.select("li.item.product.product-item")
                    print(f"  Found product grid with {len(product_items)} items")
                    
                    for item in product_items:
                        # Buscar enlaces de producto dentro de cada item
                        product_links = item.select("a[href*='.html']")
                        for link in product_links:
                            href = link.get('href')
                            if href:
                                # Normalizar URL
                                full_url = self.normalize_url(href, url)
                                if full_url and full_url not in self.product_urls:
                                    # Solo agregar si es realmente un producto
                                    if self.is_product_url(full_url):
                                        products.append(full_url)
                                        self.product_urls.add(full_url)
                                        new_products_in_page += 1
                
                # Si no encontramos productos con la estructura específica, usar selectores genéricos
                if not products:
                    print("  No products found in grid, trying generic selectors...")
                    product_selectors = [
                        "a[href*='/producto/']", "a[href*='/product/']",
                        ".product-item a", ".item a", ".product-link",
                        "a[data-product]", "a[href*='.html']",
                        ".product-card a", ".item-card a",
                        # Selectores específicos para Rapsodia
                        "li.product-item a", "a.product-item-link", 
                        "a[href*='p01']", "a[href*='p02']", "a[href*='p03']",
                        ".product-item-photo-link", "a[href*='.html']"
                    ]
                    
                    for selector in product_selectors:
                        links = soup.select(selector)
                        for link in links:
                            href = link.get('href')
                            if href:
                                # Normalizar URL
                                full_url = self.normalize_url(href, url)
                                if full_url and full_url not in self.product_urls:
                                    # Solo agregar si es realmente un producto
                                    if self.is_product_url(full_url):
                                        products.append(full_url)
                                        self.product_urls.add(full_url)
                                        new_products_in_page += 1
                
                # Buscar paginación usando el nuevo método
                pagination_links = self.extract_pagination_links(soup, url)
                next_page = self.find_next_page_url(pagination_links, url)
                
                # Si no encontramos paginación tradicional pero hay JavaScript pagination
                if not next_page and (js_pagination['has_load_more'] or js_pagination['has_infinite_scroll']):
                    print(f"  🔍 Using JavaScript pagination detection")
                    # Para JavaScript pagination, podríamos necesitar simular clicks o scroll
                    # Por ahora, marcamos que hay más contenido disponible
                    next_page = "javascript_pagination_detected"
                
                if next_page:
                    print(f"  Found next page: {next_page}")
                else:
                    print(f"  No next page found (total pagination links: {len(pagination_links)})")
            
            # Actualizar tracking de progreso
            if new_products_in_page == 0:
                self.consecutive_empty_pages[category_name] += 1
                print(f"  📊 Página sin productos nuevos (consecutivas: {self.consecutive_empty_pages[category_name]})")
            else:
                self.consecutive_empty_pages[category_name] = 0
                self.new_products_found[category_name] += new_products_in_page
                print(f"  📊 {new_products_in_page} productos nuevos encontrados")
            
            print(f"  Found {len(products)} products")
            return {"products": products, "next_page": next_page}
            
        except Exception as e:
            logger.error(f"Error parsing subcategory page {url}: {e}")
            return {"products": [], "next_page": None}

    async def parse_product_page(self, url: str, category_name: str = "UNKNOWN", subcategory_name: str = "GENERAL") -> Dict:
        """
        Parsea una página de producto y extrae toda la información detallada.
        Esta función ahora usa extract_product_details internamente para asegurar la extracción correcta de talles.
        """
        # Verificar si ya se procesó este producto
        if url in self.product_urls_extracted:
            logger.info(f"Product already processed: {url}")
            return {}
        
        print(f"Parsing product page: {url}")
        
        try:
            # Usar extract_product_details para obtener información del producto (incluye talles mejorados)
            product_info = await self.extract_product_details(url, category_name, subcategory_name)
            
            if not product_info:
                logger.warning(f"Failed to extract product info for: {url}")
                return {}
            
            # Convertir el formato de extract_product_details al formato esperado por el resto del código
            # Mapear campos si es necesario
            formatted_info = {
                'url': product_info.get('url', url),
                'category': product_info.get('category', category_name),
                'subcategory': product_info.get('subcategory', subcategory_name),
                'title': product_info.get('nombre_producto', ''),
                'price': product_info.get('precio_oferta', product_info.get('precio_regular', '')),
                'old_price': product_info.get('precio_regular') if product_info.get('precio_oferta') else None,
                'description': product_info.get('description', ''),
                'specs': {},
                'sizes': product_info.get('talles_todos', []),
                'available_sizes': product_info.get('talles_disponibles', []),
                'colors': product_info.get('colors', []),
                'image_urls': product_info.get('image_urls', []),
                'sku': product_info.get('sku', ''),
                'availability': product_info.get('availability', False)
            }
            
            # Validar datos del producto antes de agregar
            if not self.validate_product_data(formatted_info):
                logger.warning(f"Skipping invalid product: {url}")
                return {}
            
            # Marcar como procesado
            self.product_urls_extracted.add(url)
            
            # Agregar a la lista de productos por categoría y subcategoría
            if category_name == 'WOMAN':
                subcategory_key = f"WOMAN_{subcategory_name}"
                self.products_by_category[subcategory_key].append(formatted_info)
            elif category_name == 'GIRLS':
                subcategory_key = f"GIRLS_{subcategory_name}"
                self.products_by_category[subcategory_key].append(formatted_info)
            elif category_name == 'DENIM':
                subcategory_key = f"DENIM_{subcategory_name}"
                self.products_by_category[subcategory_key].append(formatted_info)
            else:
                self.products_by_category[category_name].append(formatted_info)
            
            print(f"  ✅ Added: {formatted_info.get('title', 'No title')} ({subcategory_name})")
            print(f"    💰 Precio: {formatted_info.get('price', 'N/A')}")
            if formatted_info.get('old_price'):
                print(f"    💸 Precio anterior: {formatted_info.get('old_price')}")
            print(f"    📏 Talles: {formatted_info.get('sizes', [])}")
            print(f"    ✅ Disponibles: {formatted_info.get('available_sizes', [])}")
            print(f"    🎨 Colores: {formatted_info.get('colors', [])}")
            print(f"    📸 Imágenes: {len(formatted_info.get('image_urls', []))}")
            print(f"    🏷️ SKU: {formatted_info.get('sku', 'N/A')}")
            print(f"    📦 Disponible: {'✅' if formatted_info.get('availability') else '❌'}")
            
            return formatted_info
            
        except Exception as e:
            logger.error(f"Error parsing product page {url}: {e}")
            return {}
    
    async def crawl_with_pagination(self, start_url: str, max_pages: int = 5):
        """
        Inicia el proceso de crawling desde una URL con paginación
        """
        print(f"Starting crawl from: {start_url}")
        
        category_name = self.get_category_name_from_url(start_url)
        
        # Inicializar tracking para esta categoría
        self.category_progress[category_name] = {
            'started': True,
            'pages_processed': 0,
            'products_found': 0
        }
        
        # Actualizar estructura de categorías
        self.category_structure[category_name]['url'] = start_url
        
        # Primero parsear la página principal/categoría
        category_data = await self.parse_category_page(start_url)
        
        # Procesar subcategorías encontradas
        for subcategory_url in category_data["subcategories"]:
            print(f"\nProcessing subcategory: {subcategory_url}")
            subcategory_name = self.get_subcategory_name_from_url(subcategory_url)
            self.category_structure[category_name]['subcategories'].add(subcategory_url)
            
            # Verificar si la categoría está completa antes de continuar
            if self.is_category_complete(category_name):
                print(f"  🎯 Categoría {category_name} completada. Avanzando a la siguiente...")
                break
            
            await self.process_subcategory_with_pagination(subcategory_url, max_pages, category_name, subcategory_name)
        
        # Procesar productos encontrados directamente
        for product_url in category_data["products"]:
            # Verificar que sea realmente una URL de producto individual
            if self.is_product_url(product_url):
                subcategory_name = self.get_subcategory_name_from_url(product_url)
                await self.parse_product_page(product_url, category_name, subcategory_name)
            else:
                logger.warning(f"Skipping non-product URL: {product_url}")
    
    def detect_javascript_pagination(self, soup: BeautifulSoup) -> Dict[str, any]:
        """
        Detecta si la página usa JavaScript para paginación (infinite scroll, cargar más, etc.)
        """
        js_pagination = {
            'has_load_more': False,
            'has_infinite_scroll': False,
            'load_more_selectors': [],
            'scroll_selectors': []
        }
        
        # Buscar botones de "cargar más" o "ver más"
        load_more_selectors = [
            "button[data-action*='load']", "a[data-action*='load']",
            ".load-more", ".show-more", ".view-more", ".load-more-btn",
            "button[class*='load']", "a[class*='load']",
            "button[onclick*='load']", "a[onclick*='load']",
            "button[data-load]", "a[data-load]",
            ".pagination-load-more", ".infinite-scroll-trigger"
        ]
        
        for selector in load_more_selectors:
            elements = soup.select(selector)
            if elements:
                js_pagination['has_load_more'] = True
                js_pagination['load_more_selectors'].extend([elem.get_text().strip() for elem in elements])
                print(f"  🔍 Found load more button: {elements[0].get_text().strip()}")
        
        # Buscar indicadores de infinite scroll
        scroll_selectors = [
            "[data-infinite-scroll]", "[data-scroll]", "[data-lazy]",
            ".infinite-scroll", ".lazy-load", ".scroll-trigger",
            "[data-load-more]", "[data-next-page]"
        ]
        
        for selector in scroll_selectors:
            elements = soup.select(selector)
            if elements:
                js_pagination['has_infinite_scroll'] = True
                js_pagination['scroll_selectors'].extend([elem.get('data-action', '') for elem in elements])
                print(f"  🔍 Found infinite scroll indicator: {elements[0].get('data-action', '')}")
        
        # Buscar scripts que manejen paginación
        scripts = soup.find_all('script')
        for script in scripts:
            script_content = script.get_text().lower()
            if any(keyword in script_content for keyword in ['loadmore', 'infinite', 'scroll', 'pagination', 'ajax']):
                print(f"  🔍 Found pagination script")
                js_pagination['has_load_more'] = True
        
        return js_pagination
    
    def should_stop_pagination(self, category_name: str, products_found: int, consecutive_empty: int, total_pages: int) -> bool:
        """
        Determina si deberíamos detener la paginación basándose en múltiples criterios
        """
        # Si no hemos encontrado productos en las últimas 3 páginas consecutivas
        if consecutive_empty >= 3:
            print(f"  🎯 Deteniendo paginación: 3 páginas consecutivas sin productos")
            return True
        
        # Si hemos procesado más de 15 páginas sin encontrar nuevos productos
        if products_found == 0 and total_pages > 15:
            print(f"  🎯 Deteniendo paginación: 15+ páginas sin productos")
            return True
        
        # Si hemos encontrado más de 300 productos en esta categoría
        if len(self.products_by_category[category_name]) > 300:
            print(f"  🎯 Deteniendo paginación: 300+ productos encontrados")
            return True
        
        # Si hemos procesado más de 25 páginas en total
        if total_pages > 25:
            print(f"  🎯 Deteniendo paginación: 25+ páginas procesadas")
            return True
        
        return False

    async def process_subcategory_with_pagination(self, url: str, max_pages: int, parent_category: str, subcategory_name: str):
        """
        Procesa una subcategoría con paginación
        """
        current_url = url
        page_count = 0
        consecutive_empty_pages = 0
        total_products_found = 0
        javascript_pagination_detected = False
        
        while current_url and page_count < max_pages:
            page_count += 1
            print(f"  Page {page_count}: {current_url}")
            
            # Verificar si la categoría está completa
            if self.is_category_complete(parent_category):
                print(f"  🎯 Categoría {parent_category} completada. Deteniendo paginación...")
                break
            
            subcategory_data = await self.parse_subcategory_page(current_url, parent_category)
            
            # Actualizar progreso
            self.category_progress[parent_category]['pages_processed'] += 1
            
            # Procesar productos de esta página
            products_found = len(subcategory_data["products"])
            total_products_found += products_found
            
            # Filtrar URLs válidas de productos
            valid_product_urls = set()
            for product_url in subcategory_data["products"]:
                # Verificar que sea realmente una URL de producto individual
                if self.is_product_url(product_url):
                    valid_product_urls.add(product_url)
                else:
                    logger.warning(f"Skipping non-product URL in subcategory: {product_url}")
            
            # Procesar productos en lote usando extract_product_details
            if valid_product_urls:
                await self.process_products(parent_category, subcategory_name, valid_product_urls)
            
            # Verificar si encontramos productos
            if products_found == 0:
                consecutive_empty_pages += 1
                print(f"  📊 Página sin productos (consecutivas: {consecutive_empty_pages})")
                
                # Si tenemos 2 páginas consecutivas sin productos, validar la siguiente página
                if consecutive_empty_pages >= 2 and subcategory_data["next_page"]:
                    print(f"  🔍 Validando siguiente página antes de continuar...")
                    if not await self.validate_page_has_products(subcategory_data["next_page"]):
                        print(f"  🎯 Siguiente página no tiene productos. Completando categoría...")
                        break
            else:
                consecutive_empty_pages = 0
                print(f"  📊 {products_found} productos encontrados en esta página")
            
            # Verificar si deberíamos detener la paginación
            if self.should_stop_pagination(parent_category, products_found, consecutive_empty_pages, page_count):
                break
            
            # Manejar la siguiente página
            next_page = subcategory_data["next_page"]
            
            # Si detectamos paginación JavaScript
            if next_page == "javascript_pagination_detected":
                if not javascript_pagination_detected:
                    print(f"  🔍 JavaScript pagination detected. Limiting to {max_pages} pages for this subcategory.")
                    javascript_pagination_detected = True
                
                # Para paginación JavaScript, simular que hay una siguiente página
                # pero limitar el número de páginas para evitar loops infinitos
                if page_count < max_pages:
                    # Crear URL válida para la siguiente página
                    next_url = self.create_next_page_url(current_url, page_count + 1)
                    if next_url:
                        current_url = next_url
                    else:
                        print(f"  🎯 Could not create valid next page URL. Stopping.")
                        break
                else:
                    print(f"  🎯 Reached max pages for JavaScript pagination. Stopping.")
                    break
            elif next_page:
                # Paginación tradicional
                current_url = next_page
            else:
                # No hay más páginas
                print(f"  🎯 No more pages available. Completing subcategory.")
                break
            
            # Pausa entre páginas para no sobrecargar el servidor
            if current_url and current_url != url:
                await asyncio.sleep(1)
        
        print(f"  ✅ Subcategoría {subcategory_name} completada: {total_products_found} productos en {page_count} páginas")
        if javascript_pagination_detected:
            print(f"  📝 Note: JavaScript pagination was used for this subcategory")
    
    def generate_category_mapping_markdown(self):
        """
        Genera un markdown con el mapping de categorías y subcategorías
        """
        try:
            content = "# Rapsodia - Estructura de Categorías\n\n"
            content += f"*Generado el: {time.strftime('%Y-%m-%d %H:%M:%S')}*\n\n"
            
            if self.interrupted:
                content += "⚠️ **NOTA:** Este archivo fue generado después de una interrupción del scraping.\n\n"
            
            # Agregar estadísticas de errores
            content += "## Estadísticas de Errores\n\n"
            content += f"- **Requests exitosos:** {self.successful_requests}\n"
            content += f"- **Requests fallidos:** {self.failed_requests}\n"
            content += f"- **Total de requests:** {self.successful_requests + self.failed_requests}\n\n"
            
            if self.error_counts:
                content += "### Errores por contexto:\n"
                for context, count in self.error_counts.items():
                    content += f"- **{context}:** {count} errores\n"
                content += "\n"
            
            if self.timeout_counts:
                content += "### Timeouts por contexto:\n"
                for context, count in self.timeout_counts.items():
                    content += f"- **{context}:** {count} timeouts\n"
                content += "\n"
            
            content += "---\n\n"
            
            for category_name, category_data in self.category_structure.items():
                content += f"## {category_name}\n\n"
                content += f"**URL Principal:** {category_data['url']}\n\n"
                
                # Agregar información de progreso
                if category_name in self.category_progress:
                    progress = self.category_progress[category_name]
                    content += f"**Progreso:** {progress['pages_processed']} páginas procesadas\n"
                    
                    # Contar productos válidos en esta categoría
                    valid_products = 0
                    for subcategory_key, products in self.products_by_category.items():
                        if subcategory_key.startswith(category_name):
                            valid_products += len([p for p in products if self.validate_product_data(p)])
                    
                    content += f"**Productos válidos encontrados:** {valid_products}\n\n"
                
                if category_data['subcategories']:
                    content += "### Subcategorías:\n\n"
                    for subcategory_url in sorted(category_data['subcategories']):
                        subcategory_name = self.get_subcategory_name_from_url(subcategory_url)
                        content += f"- **{subcategory_name}:** {subcategory_url}\n"
                    content += "\n"
                
                if category_data['products']:
                    content += f"### Productos Directos: {len(category_data['products'])}\n\n"
                    for product_url in sorted(category_data['products']):
                        content += f"- {product_url}\n"
                    content += "\n"
                
                content += "---\n\n"
            
            # Guardar el archivo
            with open("rapsodia_category_mapping.md", "w", encoding="utf-8") as f:
                f.write(content)
            
            print("✓ Category mapping saved to: rapsodia_category_mapping.md")
            
        except Exception as e:
            logger.error(f"Error generating category mapping: {e}")
    
    def generate_product_content(self, product: Dict) -> str:
        """
        Genera el contenido markdown para un producto individual
        """
        content = ""
        
        if product['title']:
            content += f"**Título:** {product['title']}\n\n"
        
        if product['price']:
            content += f"**Precio:** {product['price']}\n\n"
        
        if product.get('old_price'):
            content += f"**Precio anterior:** {product['old_price']}\n\n"
        
        if product['description']:
            content += f"**Descripción:** {product['description']}\n\n"
        
        if product.get('specs'):
            content += "**Especificaciones:**\n"
            for key, value in product['specs'].items():
                content += f"- **{key}:** {value}\n"
            content += "\n"
        
        if product.get('sizes'):
            content += f"**Talles disponibles:** {', '.join(product['sizes'])}\n\n"
        
        if product.get('available_sizes'):
            content += f"**Talles habilitados:** {', '.join(product['available_sizes'])}\n\n"
        
        if product.get('colors'):
            content += f"**Colores:** {', '.join(product['colors'])}\n\n"
        
        if product.get('sku'):
            content += f"**SKU:** {product['sku']}\n\n"
        
        if product.get('availability') is not None:
            content += f"**Disponible:** {'✅ Sí' if product['availability'] else '❌ No'}\n\n"
        
        if product.get('image_urls'):
            content += "**Imágenes:**\n"
            for img_url in product['image_urls']:
                content += f"- {img_url}\n"
            content += "\n"
        
        return content
    
    def generate_products_by_category_markdown(self):
        """
        Genera markdowns separados por categoría con todos los productos
        """
        try:
            files_created = 0
            total_products_saved = 0
            
            for category_name, products in self.products_by_category.items():
                # Validar que la categoría tenga productos válidos
                if not self.validate_category_has_products(category_name):
                    logger.warning(f"Skipping category {category_name} - no valid products")
                    continue
                
                # Filtrar solo productos válidos
                valid_products = [p for p in products if self.validate_product_data(p)]
                if not valid_products:
                    logger.warning(f"No valid products found for category {category_name}")
                    continue
                
                # Determinar el tipo de categoría y generar contenido
                if category_name.startswith('WOMAN_'):
                    subcategory_name = category_name.replace('WOMAN_', '')
                    category_type = "WOMAN"
                    filename = f"rapsodia_products_woman_{subcategory_name.lower()}.md"
                elif category_name.startswith('GIRLS_'):
                    subcategory_name = category_name.replace('GIRLS_', '')
                    category_type = "GIRLS"
                    filename = f"rapsodia_products_girls_{subcategory_name.lower()}.md"
                elif category_name.startswith('DENIM_'):
                    subcategory_name = category_name.replace('DENIM_', '')
                    category_type = "DENIM"
                    filename = f"rapsodia_products_denim_{subcategory_name.lower()}.md"
                else:
                    subcategory_name = "GENERAL"
                    category_type = category_name
                    filename = f"rapsodia_products_{category_name.lower()}.md"
                
                # Generar contenido del archivo
                content = f"# Rapsodia - Productos {category_type} - {subcategory_name}\n\n"
                content += f"*Generado el: {time.strftime('%Y-%m-%d %H:%M:%S')}*\n\n"
                content += f"**Categoría:** {category_type}\n"
                content += f"**Subcategoría:** {subcategory_name}\n"
                content += f"**Total de productos válidos:** {len(valid_products)}\n\n"
                
                # Agregar cada producto
                for i, product in enumerate(valid_products, 1):
                    content += f"## Producto {i}\n\n"
                    content += f"**URL:** {product['url']}\n\n"
                    content += self.generate_product_content(product)
                    content += "---\n\n"
                
                # Guardar el archivo
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(content)
                
                print(f"✓ Products for {category_type} {subcategory_name} saved to: {filename}")
                files_created += 1
                total_products_saved += len(valid_products)
            
            print(f"✓ Generated {files_created} markdown files with {total_products_saved} total valid products")
            
        except Exception as e:
            logger.error(f"Error generating products markdown: {e}")
    
    def save_statistics(self):
        """
        Guarda estadísticas del crawling
        """
        try:
            # Contar productos válidos por categoría
            valid_products_by_category = {}
            total_valid_products = 0
            
            for category_name, products in self.products_by_category.items():
                valid_products = [p for p in products if self.validate_product_data(p)]
                valid_products_by_category[category_name] = len(valid_products)
                total_valid_products += len(valid_products)
            
            stats = {
                "total_urls_discovered": len(self.discovered_urls),
                "total_product_urls": len(self.product_urls),
                "total_category_urls": len(self.category_urls),
                "categories_found": list(self.category_structure.keys()),
                "products_by_category": valid_products_by_category,
                "total_valid_products": total_valid_products,
                "interrupted": self.interrupted,
                "category_progress": self.category_progress,
                "error_statistics": {
                    "successful_requests": self.successful_requests,
                    "failed_requests": self.failed_requests,
                    "error_counts": dict(self.error_counts),
                    "timeout_counts": dict(self.timeout_counts)
                },
                "discovered_urls": list(self.discovered_urls),
                "product_urls": list(self.product_urls),
                "category_urls": list(self.category_urls)
            }
            
            with open("rapsodia_crawling_statistics.json", "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            
            print(f"\n=== Crawling Statistics ===")
            print(f"Total URLs discovered: {len(self.discovered_urls)}")
            print(f"Total product URLs: {len(self.product_urls)}")
            print(f"Categories found: {len(self.category_structure)}")
            print(f"Total valid products: {total_valid_products}")
            print(f"Successful requests: {self.successful_requests}")
            print(f"Failed requests: {self.failed_requests}")
            
            for cat, products in valid_products_by_category.items():
                if products > 0:
                    print(f"  {cat}: {products} valid products")
            
            print(f"Statistics saved to: rapsodia_crawling_statistics.json")
            
        except Exception as e:
            logger.error(f"Error saving statistics: {e}")
    
    def extract_price_number(self, price_text: str) -> float:
        """Extrae el número del precio desde el texto"""
        if not price_text:
            return 0.0
        
        # Remover símbolos y espacios, dejar solo números y punto
        price_clean = re.sub(r'[^\d.,]', '', price_text)
        # Reemplazar coma por punto si es necesario
        price_clean = price_clean.replace(',', '.')
        
        try:
            return float(price_clean)
        except:
            return 0.0
    
    def generate_supabase_data(self):
        """Genera datos estructurados para Supabase en formato JSON"""
        try:
            print("\n=== Generating Supabase Data ===")
            from datetime import datetime
            now = datetime.now().isoformat()
            
            # Datos para las tablas
            brands_data = []
            items_data = []
            item_colors_data = []
            item_images_data = []
            colors_mapping = {}  # Para mapear nombres de colores a UUIDs
            
            # Brand de Rapsodia
            brand_id = "00000000-0000-0000-0000-000000000002"  # UUID fijo para Rapsodia
            brands_data.append({
                "brand_id": brand_id,
                "name": "Rapsodia",
                "logo": None,
                "url": "https://www.rapsodia.com.ar",
                "created_at": now,
                "updated_at": now
            })
            
            # Procesar todos los productos
            for category_name, products in self.products_by_category.items():
                valid_products = [p for p in products if self.validate_product_data(p)]
                
                for product in valid_products:
                    # Generar UUID para el item (usando un hash del URL para consistencia)
                    url_hash = hashlib.md5(product['url'].encode()).hexdigest()
                    item_id = f"{url_hash[:8]}-{url_hash[8:12]}-{url_hash[12:16]}-{url_hash[16:20]}-{url_hash[20:32]}"
                    
                    # Extraer precio numérico
                    price_value = self.extract_price_number(product.get('price', '0'))
                    
                    # Determinar categoría completa
                    if category_name.startswith('WOMAN_'):
                        subcategory_name = category_name.replace('WOMAN_', '')
                        full_category = f"WOMAN - {subcategory_name}"
                    elif category_name.startswith('GIRLS_'):
                        subcategory_name = category_name.replace('GIRLS_', '')
                        full_category = f"GIRLS - {subcategory_name}"
                    elif category_name.startswith('DENIM_'):
                        subcategory_name = category_name.replace('DENIM_', '')
                        full_category = f"DENIM - {subcategory_name}"
                    else:
                        full_category = category_name
                    
                    # Crear item
                    item = {
                        "item_id": item_id,
                        "name": product.get('title', 'Sin título'),
                        "description": product.get('description', ''),
                        "price": price_value,
                        "available": product.get('availability', True),
                        "brand_id": brand_id,
                        "category": full_category,
                        "sizes": product.get('sizes', []),
                        "sizes_available": product.get('available_sizes', []),
                        "created_at": now,
                        "updated_at": now
                    }
                    items_data.append(item)
                    
                    # Procesar colores
                    for color_name in product.get('colors', []):
                        if not color_name:
                            continue
                        
                        # Crear o obtener color_id
                        if color_name not in colors_mapping:
                            color_hash = hashlib.md5(color_name.encode()).hexdigest()
                            color_id = f"{color_hash[:8]}-{color_hash[8:12]}-{color_hash[12:16]}-{color_hash[16:20]}-{color_hash[20:32]}"
                            colors_mapping[color_name] = color_id
                        else:
                            color_id = colors_mapping[color_name]
                        
                        # Crear relación item-color
                        item_color_hash = hashlib.md5(f"{item_id}{color_id}".encode()).hexdigest()
                        item_color_id = f"{item_color_hash[:8]}-{item_color_hash[8:12]}-{item_color_hash[12:16]}-{item_color_hash[16:20]}-{item_color_hash[20:32]}"
                        
                        item_colors_data.append({
                            "item_color_id": item_color_id,
                            "item_id": item_id,
                            "color_id": color_id,
                            "created_at": now,
                            "updated_at": None
                        })
                    
                    # Procesar imágenes
                    image_urls = product.get('image_urls', [])
                    for idx, img_url in enumerate(image_urls):
                        if not img_url:
                            continue
                        
                        # Determinar color_id si hay colores asociados
                        color_id_for_image = None
                        if product.get('colors') and len(product.get('colors', [])) > 0:
                            # Intentar asociar imagen con color basado en el índice
                            color_index = idx % len(product.get('colors', []))
                            color_name = product.get('colors', [])[color_index]
                            if color_name in colors_mapping:
                                color_id_for_image = colors_mapping[color_name]
                        
                        img_hash = hashlib.md5(f"{item_id}{img_url}{idx}".encode()).hexdigest()
                        image_id = f"{img_hash[:8]}-{img_hash[8:12]}-{img_hash[12:16]}-{img_hash[16:20]}-{img_hash[20:32]}"
                        
                        item_images_data.append({
                            "itemimage_id": image_id,
                            "item_id": item_id,
                            "url": img_url,
                            "is_primary": (idx == 0),  # Primera imagen es primaria
                            "color_id": color_id_for_image,
                            "created_at": now
                        })
            
            # Generar datos de colores únicos
            item_colors_list = []
            for color_name, color_id in colors_mapping.items():
                item_colors_list.append({
                    "color_id": color_id,
                    "name": color_name,
                    "created_at": now,
                    "updated_at": None
                })
            
            # Guardar todos los datos en archivos JSON
            supabase_data = {
                "brands": brands_data,
                "item_colors": item_colors_list,
                "items": items_data,
                "item_x_item_colors": item_colors_data,
                "item_images": item_images_data
            }
            
            # Guardar archivo principal
            with open("rapsodia_supabase_data.json", "w", encoding="utf-8") as f:
                json.dump(supabase_data, f, indent=2, ensure_ascii=False)
            
            # Guardar archivos separados por tabla
            with open("rapsodia_supabase_brands.json", "w", encoding="utf-8") as f:
                json.dump(brands_data, f, indent=2, ensure_ascii=False)
            
            with open("rapsodia_supabase_item_colors.json", "w", encoding="utf-8") as f:
                json.dump(item_colors_list, f, indent=2, ensure_ascii=False)
            
            with open("rapsodia_supabase_items.json", "w", encoding="utf-8") as f:
                json.dump(items_data, f, indent=2, ensure_ascii=False)
            
            with open("rapsodia_supabase_item_x_item_colors.json", "w", encoding="utf-8") as f:
                json.dump(item_colors_data, f, indent=2, ensure_ascii=False)
            
            with open("rapsodia_supabase_item_images.json", "w", encoding="utf-8") as f:
                json.dump(item_images_data, f, indent=2, ensure_ascii=False)
            
            print(f"✓ Supabase data generated:")
            print(f"  - Brands: {len(brands_data)}")
            print(f"  - Item Colors: {len(item_colors_list)}")
            print(f"  - Items: {len(items_data)}")
            print(f"  - Item-Color relations: {len(item_colors_data)}")
            print(f"  - Item Images: {len(item_images_data)}")
            print(f"✓ Files saved:")
            print(f"  - rapsodia_supabase_data.json (all data)")
            print(f"  - rapsodia_supabase_brands.json")
            print(f"  - rapsodia_supabase_item_colors.json")
            print(f"  - rapsodia_supabase_items.json")
            print(f"  - rapsodia_supabase_item_x_item_colors.json")
            print(f"  - rapsodia_supabase_item_images.json")
            
        except Exception as e:
            logger.error(f"Error generating Supabase data: {e}")
            print(f"❌ Error generating Supabase data: {e}")
    
    def save_all_data(self):
        """
        Guarda todos los datos recolectados
        """
        try:
            print("\n=== Saving All Data ===")
            
            # Verificar que hay datos para guardar
            total_valid_products = sum(
                len([p for p in products if self.validate_product_data(p)])
                for products in self.products_by_category.values()
            )
            
            if total_valid_products == 0:
                logger.warning("No valid products found to save")
                print("⚠️ No valid products found to save")
                return
            
            self.generate_category_mapping_markdown()
            self.generate_products_by_category_markdown()
            self.save_statistics()
            self.generate_supabase_data()  # Generar datos para Supabase
            
            print(f"✓ All data saved successfully ({total_valid_products} valid products)")
            
        except Exception as e:
            logger.error(f"Error saving all data: {e}")
            print(f"❌ Error saving data: {e}")
    
    async def extract_all_product_info(self, soup: BeautifulSoup, url: str, category_name: str, subcategory_name: str) -> Dict:
        """
        Extrae toda la información detallada del producto usando selectores CSS específicos de Magento/Rapsodia
        
        Args:
            soup: BeautifulSoup object del HTML
            url: URL del producto
            category_name: Nombre de la categoría
            subcategory_name: Nombre de la subcategoría
            
        Returns:
            Dict con toda la información detallada del producto
        """
        product_info = {
            'url': url,
            'category': category_name,
            'subcategory': subcategory_name,
            'title': '',
            'price': '',
            'old_price': '',
            'description': '',
            'specs': {},
            'sizes': [],
            'available_sizes': [],
            'colors': [],
            'image_urls': [],
            'sku': '',
            'availability': False
        }
        
        try:
            # 1. Extraer título del producto (selector específico de Magento)
            title_elem = soup.select_one("h1.page-title span.base")
            if title_elem:
                product_info["title"] = title_elem.get_text().strip()
            else:
                # Fallback a selectores genéricos
                for selector in ["h1.page-title", "h1.product-name", "h1"]:
                    title_elem = soup.select_one(selector)
                    if title_elem:
                        product_info["title"] = title_elem.get_text().strip()
                        break
            
            # 2. Extraer precio final (selector específico de Magento)
            price_elem = soup.select_one("span.special-price span.price-container.price-final_price span.price-wrapper.price-including-tax span.price")
            if price_elem:
                product_info["price"] = price_elem.get_text().strip()
            else:
                # Fallback a selectores genéricos
                for selector in [".price", ".product-price", "[data-price-type='finalPrice'] .price"]:
                    price_elem = soup.select_one(selector)
                    if price_elem:
                        product_info["price"] = price_elem.get_text().strip()
                        break
            
            # 3. Extraer precio anterior (si hay descuento)
            old_price_elem = soup.select_one("span.old-price span.price-container.price-final_price span.price-wrapper.price-including-tax span.price")
            if old_price_elem:
                product_info["old_price"] = old_price_elem.get_text().strip()
            
            # 4. Extraer descripción completa
            desc_elem = soup.select_one("div.product.attribute.overview div.value")
            if desc_elem:
                # Remover elementos no deseados de la descripción
                for unwanted in desc_elem.select("ul.data.additional-attributes, br"):
                    unwanted.decompose()
                product_info["description"] = desc_elem.get_text().strip()
            
            # 5. Extraer especificaciones
            specs_elem = soup.select_one("ul.data.additional-attributes")
            if specs_elem:
                for li_elem in specs_elem.select("li"):
                    label_elem = li_elem.select_one("h5.label")
                    data_elem = li_elem.select_one("p.data")
                    if label_elem and data_elem:
                        label = label_elem.get_text().strip()
                        data = data_elem.get_text().strip()
                        if label and data:
                            product_info["specs"][label] = data
            
            # 6. Extraer talles disponibles (selector específico de Magento)
            # Selectores priorizados según especificidad y confiabilidad
            # Se detiene la búsqueda tan pronto como un selector devuelve elementos
            size_selectors = [
                # Prioridad 1: El más específico - usa data-attribute-code (más confiable)
                "div.swatch-attribute[data-attribute-code='talle_rap'] div.swatch-option.text",
                # Prioridad 2: Usa la clase CSS talle_rap
                "div.swatch-attribute.talle_rap div.swatch-option.text",
                # Prioridad 3: Selector genérico con data-option-label (más seguro)
                "div.swatch-attribute-options div.swatch-option.text[data-option-label]",
                # Prioridad 4: Selector genérico sin data-option-label
                "div.swatch-attribute-options div.swatch-option.text"
            ]
            
            size_elements = []
            selector_used = None
            # Iterar y buscar - detener tan pronto como se encuentren elementos
            for selector in size_selectors:
                elements = soup.select(selector)
                if elements:
                    size_elements = elements
                    selector_used = selector
                    logger.debug(f"Talles encontrados con selector: {selector} ({len(elements)} elementos)")
                    break
            
            # Si no encontramos elementos con los selectores principales, buscar de forma más amplia
            if not size_elements:
                logger.debug(f"No se encontraron talles con selectores principales, buscando de forma más amplia...")
                # Buscar cualquier elemento que contenga talles
                all_elements = soup.find_all('div', class_='swatch-option')
                for element in all_elements:
                    # Verificar si el elemento contiene texto que parece un talle
                    size_text = element.get_text().strip()
                    if size_text and (any(char.isdigit() for char in size_text) or size_text in ['XS', 'S', 'M', 'L', 'XL', 'XXL']):
                        size_elements.append(element)
            
            # Extraer valores del atributo data-option-label (fuente más confiable)
            for element in size_elements:
                # Priorizar data-option-label como fuente principal (según instrucciones)
                size_text = element.get('data-option-label')
                
                # Si no hay data-option-label, usar texto interno como respaldo
                if not size_text:
                    size_text = element.get_text().strip()
                
                # Último recurso: otros atributos
                if not size_text:
                    size_text = element.get('aria-label') or element.get('data-option-tooltip-value')
                
                if size_text:
                    clean_size = self.clean_size_text(size_text)
                    if clean_size and clean_size not in product_info["sizes"]:
                        product_info["sizes"].append(clean_size)
                        
                        # Verificar si el elemento está deshabilitado
                        is_disabled = self.is_element_disabled(element)
                        
                        # Solo agregar a disponibles si NO está deshabilitado
                        if not is_disabled:
                            product_info["available_sizes"].append(clean_size)
                        else:
                            # Log para debugging
                            logger.debug(f"Talle deshabilitado encontrado: {clean_size} en {url}")
            
            # Log para debugging
            if product_info["sizes"]:
                logger.info(f"Talles encontrados: {product_info['sizes']} - Disponibles: {product_info['available_sizes']}")
            else:
                logger.warning(f"No se encontraron talles en {url}")
            
            # 7. Extraer colores disponibles (selector específico de Magento)
            color_elements = soup.select("div.custom-swatches.swatch-attribute.color div.swatch-attribute-options div.swatch-option.color")
            for element in color_elements:
                color_text = element.get('aria-label')
                if color_text and color_text not in product_info["colors"]:
                    product_info["colors"].append(color_text)
            
            # 8. Extraer múltiples URLs de imágenes de alta resolución
            # Método 1: Extraer de JSON-LD (más confiable)
            json_ld_scripts = soup.find_all("script", type="application/ld+json")
            for script in json_ld_scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and 'image' in data:
                        if isinstance(data['image'], list):
                            for img_url in data['image']:
                                if img_url and img_url not in product_info["image_urls"]:
                                    product_info["image_urls"].append(img_url)
                        elif isinstance(data['image'], str):
                            if data['image'] not in product_info["image_urls"]:
                                product_info["image_urls"].append(data['image'])
                except Exception as e:
                    logger.debug(f"Error parsing JSON-LD: {e}")
                    continue
            
            # Método 2: Extraer de scripts de Magento (galería de productos)
            if len(product_info["image_urls"]) < 2:  # Si no tenemos suficientes imágenes
                scripts = soup.find_all("script", type="text/x-magento-init")
                for script in scripts:
                    try:
                        script_text = script.string
                        if script_text and ("gallery" in script_text or "product" in script_text):
                            # Buscar patrones de URLs de imágenes en scripts de Magento
                            import re
                            # Patrones más específicos para Rapsodia
                            img_patterns = [
                                r'https://[^"\']*\.(?:jpg|jpeg|png|webp)',
                                r'https://[^"\']*rapsodia[^"\']*\.(?:jpg|jpeg|png|webp)',
                                r'https://[^"\']*media[^"\']*\.(?:jpg|jpeg|png|webp)',
                                r'https://[^"\']*catalog[^"\']*\.(?:jpg|jpeg|png|webp)'
                            ]
                            
                            for pattern in img_patterns:
                                img_urls = re.findall(pattern, script_text)
                                for img_url in img_urls:
                                    if img_url and img_url not in product_info["image_urls"]:
                                        product_info["image_urls"].append(img_url)
                    except Exception as e:
                        logger.debug(f"Error parsing Magento script: {e}")
                        continue
            
            # Método 3: Extraer de galería Fotorama (específico para Rapsodia)
            if len(product_info["image_urls"]) < 10:  # Si aún no tenemos suficientes imágenes
                # Buscar específicamente dentro del contenedor .product.media
                product_media = soup.select_one("div.product.media")
                if product_media:
                    logger.info(f"Encontrado contenedor .product.media en {url}")
                    
                    # 3.1: Extraer de elementos fotorama__stage__frame (imágenes principales)
                    fotorama_frames = product_media.select("div.fotorama__stage__frame")
                    logger.info(f"Encontrados {len(fotorama_frames)} elementos fotorama__stage__frame")
                    for frame in fotorama_frames:
                        frame_href = frame.get('href')
                        if frame_href:
                            normalized_src = self.normalize_url(frame_href, url)
                            if normalized_src and normalized_src not in product_info["image_urls"]:
                                product_info["image_urls"].append(normalized_src)
                                logger.debug(f"Agregada imagen principal: {normalized_src}")
                    
                    # 3.2: Extraer de elementos fotorama__nav__frame (miniaturas)
                    fotorama_thumbs = product_media.select("div.fotorama__nav__frame img.fotorama__img")
                    logger.info(f"Encontrados {len(fotorama_thumbs)} elementos fotorama__nav__frame img.fotorama__img")
                    for thumb in fotorama_thumbs:
                        img_src = thumb.get('src')
                        if img_src:
                            normalized_src = self.normalize_url(img_src, url)
                            if normalized_src and normalized_src not in product_info["image_urls"]:
                                product_info["image_urls"].append(normalized_src)
                                logger.debug(f"Agregada miniatura: {normalized_src}")
                    
                    # 3.3: Extraer de elementos zoom-image-link (imágenes de zoom)
                    zoom_links = product_media.select("a.zoom-image-link")
                    logger.info(f"Encontrados {len(zoom_links)} elementos zoom-image-link")
                    for zoom_link in zoom_links:
                        zoom_src = zoom_link.get('data-src')
                        if zoom_src:
                            normalized_src = self.normalize_url(zoom_src, url)
                            if normalized_src and normalized_src not in product_info["image_urls"]:
                                product_info["image_urls"].append(normalized_src)
                                logger.debug(f"Agregada imagen de zoom: {normalized_src}")
                    
                    # 3.4: Extraer de elementos img.fotorama__img directamente
                    fotorama_imgs = product_media.select("img.fotorama__img")
                    logger.info(f"Encontrados {len(fotorama_imgs)} elementos img.fotorama__img")
                    for img in fotorama_imgs:
                        img_src = img.get('src')
                        if img_src:
                            normalized_src = self.normalize_url(img_src, url)
                            if normalized_src and normalized_src not in product_info["image_urls"]:
                                product_info["image_urls"].append(normalized_src)
                                logger.debug(f"Agregada imagen fotorama: {normalized_src}")
                    
                    # 3.5: Extraer de elementos dentro de gallery-placeholder-container
                    gallery_container = product_media.select_one("div.gallery-placeholder-container")
                    if gallery_container:
                        logger.info("Encontrado gallery-placeholder-container")
                        # Buscar todas las imágenes dentro del contenedor de galería
                        gallery_imgs = gallery_container.select("img[src*='media'], img[src*='catalog'], img.fotorama__img")
                        logger.info(f"Encontradas {len(gallery_imgs)} imágenes en gallery-placeholder-container")
                        for img in gallery_imgs:
                            img_src = img.get('src')
                            if img_src:
                                normalized_src = self.normalize_url(img_src, url)
                                if normalized_src and normalized_src not in product_info["image_urls"]:
                                    product_info["image_urls"].append(normalized_src)
                                    logger.debug(f"Agregada imagen de galería: {normalized_src}")
                else:
                    logger.warning(f"No se encontró contenedor .product.media en {url}")
            
            # Método 4: Extraer de elementos de galería genéricos en el DOM
            if len(product_info["image_urls"]) < 8:  # Si aún no tenemos suficientes imágenes
                # Selectores específicos para galerías de productos de Rapsodia
                gallery_selectors = [
                    "img.fotorama__img",
                    ".gallery-placeholder img",
                    ".product-image-gallery img",
                    ".product-image img",
                    ".gallery-image img",
                    "img[data-src]",
                    "img[data-lazy]",
                    ".product-photo img",
                    ".product-gallery img",
                    "img[src*='rapsodia']",
                    "img[src*='media']",
                    "img[src*='catalog']",
                    ".product.media img",
                    ".gallery-wrapper img"
                ]
                
                for selector in gallery_selectors:
                    img_elements = soup.select(selector)
                    for img in img_elements:
                        # Obtener URL de imagen de diferentes atributos
                        img_src = img.get('src') or img.get('data-src') or img.get('data-lazy') or img.get('data-original')
                        if img_src:
                            normalized_src = self.normalize_url(img_src, url)
                            if normalized_src and normalized_src not in product_info["image_urls"]:
                                product_info["image_urls"].append(normalized_src)
            
            # Método 5: Extraer de atributos data de elementos de galería
            if len(product_info["image_urls"]) < 10:  # Si aún necesitamos más imágenes
                # Buscar elementos con atributos data que contengan URLs de imágenes
                data_elements = soup.find_all(attrs={"data-image": True})
                for element in data_elements:
                    data_image = element.get('data-image')
                    if data_image:
                        normalized_src = self.normalize_url(data_image, url)
                        if normalized_src and normalized_src not in product_info["image_urls"]:
                            product_info["image_urls"].append(normalized_src)
                
                # Buscar en elementos con otros atributos data
                for element in soup.find_all(attrs={"data-src": True}):
                    data_src = element.get('data-src')
                    if data_src and ('jpg' in data_src or 'jpeg' in data_src or 'png' in data_src or 'webp' in data_src):
                        normalized_src = self.normalize_url(data_src, url)
                        if normalized_src and normalized_src not in product_info["image_urls"]:
                            product_info["image_urls"].append(normalized_src)
            
            # Método 6: Extraer de meta tags (imagen principal)
            if not product_info["image_urls"]:
                meta_img = soup.find("meta", property="og:image")
                if meta_img:
                    img_url = meta_img.get('content')
                    if img_url:
                        normalized_src = self.normalize_url(img_url, url)
                        if normalized_src:
                            product_info["image_urls"].append(normalized_src)
            
            # Limpiar y validar URLs de imágenes
            cleaned_image_urls = []
            original_urls = product_info["image_urls"].copy()  # Guardar URLs originales para debugging
            
            logger.info(f"URLs originales encontradas: {len(original_urls)}")
            for i, img_url in enumerate(original_urls):
                logger.debug(f"URL original {i+1}: {img_url}")
                
                if img_url and self.validate_image_url(img_url):
                    # Para mantener unicidad, usar la URL original en lugar de limpiarla
                    # Solo limpiar parámetros muy específicos que no afecten la unicidad
                    clean_url = self.clean_image_url(img_url)
                    
                    # Verificar si la URL ya existe (comparar tanto original como limpia)
                    url_exists = False
                    for existing_url in cleaned_image_urls:
                        if clean_url == existing_url or img_url == existing_url:
                            url_exists = True
                            logger.debug(f"URL duplicada encontrada: {img_url}")
                            break
                    
                    if not url_exists:
                        cleaned_image_urls.append(clean_url)
                        logger.debug(f"URL agregada: {clean_url}")
                    else:
                        logger.debug(f"URL descartada por duplicado: {img_url}")
                else:
                    logger.debug(f"URL inválida descartada: {img_url}")
            
            product_info["image_urls"] = cleaned_image_urls
            
            # Log detallado de URLs únicas
            logger.info(f"URLs únicas después de limpieza: {len(cleaned_image_urls)}")
            for i, url in enumerate(cleaned_image_urls):
                logger.debug(f"URL única {i+1}: {url}")
            
            # 9. Extraer SKU
            sku_elem = soup.select_one("div.product.attribute.sku div.value")
            if sku_elem:
                product_info["sku"] = sku_elem.get_text().strip()
            
            # 10. Verificar disponibilidad
            stock_elem = soup.select_one("div.stock.available")
            if stock_elem and "En stock" in stock_elem.get_text():
                product_info["availability"] = True
            
            # Mantener compatibilidad con el formato anterior
            product_info["images"] = product_info["image_urls"]
            
            # Log detallado de imágenes extraídas
            image_count = len(product_info["image_urls"])
            logger.info(f"Extracted detailed product info: {product_info['title']} - SKU: {product_info['sku']} - {len(product_info['sizes'])} sizes, {len(product_info['available_sizes'])} available - {image_count} images")
            
            if image_count > 0:
                print(f"    📸 Imágenes encontradas: {image_count}")
                for i, img_url in enumerate(product_info["image_urls"][:3], 1):  # Mostrar solo las primeras 3
                    print(f"      {i}. {img_url}")
                if image_count > 3:
                    print(f"      ... y {image_count - 3} más")
            else:
                print(f"    📸 No se encontraron imágenes")
            
            return product_info
            
        except Exception as e:
            logger.error(f"Error extracting detailed product info from {url}: {e}")
            return product_info
    
    async def extract_product_details(self, url: str, category_name: str, subcategory_name: str) -> Optional[Dict]:
        """
        Parsea una página de producto individual y extrae todos los datos, incluyendo talles y stock.
        Esta función utiliza los selectores robustos para extraer talles disponibles.
        
        Args:
            url: URL del producto a extraer
            category_name: Nombre de la categoría
            subcategory_name: Nombre de la subcategoría
            
        Returns:
            Dict con la información del producto o None si hay error
        """
        logger.info(f"Extracting details for product: {url}")
        
        # 1. Usar el método seguro de crawling para obtener el HTML renderizado (con JS ejecutado)
        result = await self.safe_crawl_page(url, "product_page")
        
        if not result['success']:
            logger.error(f"Failed to crawl product page {url}: {result['error']}")
            return None
        
        soup = BeautifulSoup(result['html'], 'html.parser') if result['html'] else None
        if not soup:
            logger.warning(f"No HTML content for product: {url}")
            return None
        
        product_info = {
            'url': url,
            'category': category_name,
            'subcategory': subcategory_name,
            'talles_disponibles': [],
            'talles_todos': [],
            'precio_regular': None,
            'precio_oferta': None,
            'nombre_producto': None,
            'sku': None,
            'description': '',
            'image_urls': [],
            'colors': [],
            'availability': False
        }
        
        # 2. Extracción de Nombre y Precio (Básicos)
        nombre_elem = soup.select_one("span.base[itemprop='name']") or soup.select_one("h1.page-title span.base")
        if nombre_elem:
            product_info['nombre_producto'] = nombre_elem.get_text(strip=True)
        else:
            # Fallback a otros selectores
            for selector in ["h1.page-title", "h1.product-name", "h1"]:
                title_elem = soup.select_one(selector)
                if title_elem:
                    product_info['nombre_producto'] = title_elem.get_text(strip=True)
                    break
        
        # Intenta obtener el precio de oferta o el final (si hay)
        price_element = soup.select_one(".price-box .special-price .price") or soup.select_one(".price-box .price-final_price .price") or soup.select_one("span.special-price span.price-container.price-final_price span.price-wrapper.price-including-tax span.price")
        if price_element:
            # Limpiar el texto (quitar '$', comas y espacios, convertir a string limpio)
            price_text = price_element.get_text(strip=True)
            product_info['precio_oferta'] = price_text
            product_info['precio_regular'] = price_text  # Por ahora usar el mismo
        
        # Extraer precio regular si existe
        regular_price_elem = soup.select_one(".price-box .old-price .price") or soup.select_one(".price-box .price-regular_price .price")
        if regular_price_elem:
            product_info['precio_regular'] = regular_price_elem.get_text(strip=True)
        
        # 3. Extracción de SKU
        sku_elem = soup.select_one("div.product.attribute.sku div.value") or soup.select_one("[itemprop='sku']")
        if sku_elem:
            product_info['sku'] = sku_elem.get_text(strip=True)
        
        # 4. Extracción de TALLES DISPONIBLES (El Foco Principal)
        # Selectores en orden de prioridad
        size_selectors = [
            # El más específico: busca el atributo 'talle_rap' (talle rápido)
            "div.swatch-attribute[data-attribute-code='talle_rap'] div.swatch-option.text",
            # Respaldo: busca el contenedor de talla genérico, luego las opciones de texto
            "div.swatch-attribute.talle_rap div.swatch-option.text",
            "div.swatch-attribute div.swatch-attribute-options div.swatch-option.text",
            # Respaldo más genérico: cualquier swatch de texto con etiqueta de opción
            "div.swatch-option.text[data-option-label]"
        ]
        
        talles_raw = []
        selector_used = None
        
        for selector in size_selectors:
            talles_raw = soup.select(selector)
            if talles_raw:
                selector_used = selector
                logger.info(f"Found {len(talles_raw)} raw sizes using selector: {selector}")
                break
        
        talles_disponibles = []
        talles_todos = []
        
        for talle_element in talles_raw:
            # Priorizar data-option-label como fuente principal
            talle_label = talle_element.get('data-option-label', '').strip()
            
            # Si no hay data-option-label, usar texto interno como respaldo
            if not talle_label:
                talle_label = talle_element.get_text(strip=True)
            
            # Último recurso: otros atributos
            if not talle_label:
                talle_label = talle_element.get('aria-label', '').strip() or talle_element.get('data-option-tooltip-value', '').strip()
            
            if talle_label:
                # Limpiar y normalizar el talle
                clean_size = self.clean_size_text(talle_label)
                if clean_size:
                    # Agregar a la lista de todos los talles
                    if clean_size not in talles_todos:
                        talles_todos.append(clean_size)
                    
                    # 5. Verificar Disponibilidad (Stock)
                    # Magento usa la clase 'disabled' para talles agotados (out-of-stock)
                    is_available = not self.is_element_disabled(talle_element)
                    
                    if is_available:
                        talles_disponibles.append(clean_size)
                        logger.debug(f"Talle disponible encontrado: {clean_size}")
                    else:
                        logger.debug(f"Talle deshabilitado encontrado: {clean_size} en {url}")
        
        product_info['talles_disponibles'] = talles_disponibles
        product_info['talles_todos'] = talles_todos
        
        if not talles_disponibles:
            logger.warning(f"No available sizes found for {url}. Total raw elements found: {len(talles_raw)}")
            if talles_raw:
                logger.debug(f"All sizes found (including unavailable): {talles_todos}")
        else:
            logger.info(f"Available sizes extracted: {talles_disponibles} (Total: {talles_todos})")
        
        # 6. Extraer colores disponibles
        color_elements = soup.select("div.custom-swatches.swatch-attribute.color div.swatch-attribute-options div.swatch-option.color")
        for element in color_elements:
            color_text = element.get('aria-label') or element.get('title') or element.get_text(strip=True)
            if color_text and color_text not in product_info["colors"]:
                product_info["colors"].append(color_text)
        
        # 7. Extraer descripción
        desc_elem = soup.select_one("div.product.attribute.description div.value") or soup.select_one("[itemprop='description']")
        if desc_elem:
            product_info['description'] = desc_elem.get_text(strip=True)
        
        # 8. Extraer imágenes - Múltiples métodos para capturar todas las imágenes
        # Método 1: JSON-LD (estructurado, más confiable)
        json_ld_scripts = soup.find_all("script", type="application/ld+json")
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and 'image' in data:
                    if isinstance(data['image'], list):
                        for img in data['image']:
                            if img and img not in product_info['image_urls']:
                                product_info['image_urls'].append(img)
                    elif isinstance(data['image'], str):
                        if data['image'] not in product_info['image_urls']:
                            product_info['image_urls'].append(data['image'])
            except:
                pass
        
        # Método 2: Fotorama stage frames (div.fotorama__stage__frame con href) - Prioridad alta
        # Estos contienen las URLs completas de las imágenes en el atributo href
        fotorama_frames = soup.select("div.fotorama__stage__frame[href]")
        for frame in fotorama_frames:
            img_url = frame.get('href')
            if img_url and img_url not in product_info['image_urls']:
                product_info['image_urls'].append(img_url)
                logger.debug(f"Imagen encontrada en fotorama__stage__frame: {img_url}")
        
        # Método 3: Fotorama images (img.fotorama__img) - Incluye todas las imágenes de la galería
        fotorama_imgs = soup.select("img.fotorama__img")
        for img in fotorama_imgs:
            # Priorizar src, luego data-src
            img_url = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
            if img_url:
                # Normalizar URL (convertir URLs relativas a absolutas si es necesario)
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                elif img_url.startswith('/'):
                    img_url = self.base_url + img_url
                
                if img_url not in product_info['image_urls']:
                    product_info['image_urls'].append(img_url)
                    logger.debug(f"Imagen encontrada en fotorama__img: {img_url}")
        
        # Método 4: Fotorama nav frames (miniaturas en la navegación)
        fotorama_nav_frames = soup.select("div.fotorama__nav__frame img.fotorama__img")
        for img in fotorama_nav_frames:
            img_url = img.get('src') or img.get('data-src')
            if img_url:
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                elif img_url.startswith('/'):
                    img_url = self.base_url + img_url
                
                if img_url not in product_info['image_urls']:
                    product_info['image_urls'].append(img_url)
        
        # Método 5: Buscar en el contenedor de producto media
        product_media = soup.select_one("div.product.media")
        if product_media:
            # Buscar todas las imágenes dentro del contenedor de media
            all_media_imgs = product_media.select("img[src*='media'], img[src*='catalog'], img[src*='product']")
            for img in all_media_imgs:
                img_url = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                if img_url:
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    elif img_url.startswith('/'):
                        img_url = self.base_url + img_url
                    
                    if img_url not in product_info['image_urls']:
                        product_info['image_urls'].append(img_url)
        
        # Limpiar y normalizar URLs de imágenes
        cleaned_image_urls = []
        for img_url in product_info['image_urls']:
            if img_url:
                # Remover parámetros de query que pueden causar duplicados (como ?quality=100)
                clean_url = img_url.split('?')[0] if '?' in img_url else img_url
                # Verificar que no sea duplicado
                if clean_url not in cleaned_image_urls:
                    cleaned_image_urls.append(clean_url)
        
        product_info['image_urls'] = cleaned_image_urls
        
        if product_info['image_urls']:
            logger.info(f"Total de imágenes encontradas: {len(product_info['image_urls'])}")
        else:
            logger.warning(f"No se encontraron imágenes para {url}")
        
        # 9. Verificar disponibilidad general
        stock_elem = soup.select_one("div.stock.available") or soup.select_one("[itemprop='availability']")
        if stock_elem:
            availability_text = stock_elem.get_text(strip=True).lower()
            product_info['availability'] = 'disponible' in availability_text or 'en stock' in availability_text or 'available' in availability_text
        else:
            # Si hay talles disponibles, asumir que el producto está disponible
            product_info['availability'] = len(talles_disponibles) > 0
        
        # 10. Validación y Retorno
        if not self.validate_product_data(product_info):
            logger.warning(f"Product data validation failed for {url}")
            return None
        
        return product_info
    
    async def process_products(self, category_name: str, subcategory_name: str, product_urls_to_process: Set[str], max_concurrent: int = 5):
        """
        Procesa las URLs de productos descubiertas y extrae sus detalles.
        Procesa en lotes para evitar saturar el sistema.
        
        Args:
            category_name: Nombre de la categoría
            subcategory_name: Nombre de la subcategoría
            product_urls_to_process: Set de URLs de productos a procesar
            max_concurrent: Número máximo de productos a procesar simultáneamente (default: 5)
        """
        logger.info(f"Processing {len(product_urls_to_process)} products for {category_name}/{subcategory_name} (max concurrent: {max_concurrent})")
        
        # Filtrar URLs que ya fueron procesadas
        urls_to_process = [url for url in product_urls_to_process if url not in self.product_urls_extracted]
        
        if not urls_to_process:
            logger.info(f"No new products to process (all already extracted)")
            return
        
        # Procesar en lotes para evitar saturar el sistema
        semaphore = asyncio.Semaphore(max_concurrent)
        successful = 0
        failed = 0
        
        async def process_single_product(url: str):
            """Procesa un solo producto con control de concurrencia"""
            async with semaphore:
                try:
                    # Verificar nuevamente si ya fue procesado (evitar condiciones de carrera)
                    if url in self.product_urls_extracted:
                        return None
                    
                    product_info = await self.extract_product_details(url, category_name, subcategory_name)
                    
                    # Marcar como procesado solo después de extraer exitosamente
                    if product_info:
                        self.product_urls_extracted.add(url)
                    
                    return (url, product_info)
                except Exception as e:
                    logger.error(f"Error processing product {url}: {e}")
                    return (url, None)
        
        # Procesar en lotes reales para evitar crear demasiadas tareas a la vez
        batch_size = max_concurrent
        results = []
        
        for i in range(0, len(urls_to_process), batch_size):
            batch_urls = urls_to_process[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch_urls)} products)")
            
            # Crear tareas solo para este lote
            batch_tasks = [process_single_product(url) for url in batch_urls]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            results.extend(batch_results)
            
            # Pequeña pausa entre lotes para no sobrecargar el sistema
            if i + batch_size < len(urls_to_process):
                await asyncio.sleep(1)
        
        # Procesar resultados
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error processing product: {result}")
                failed += 1
                continue
            
            if result is None:
                continue
            
            url, product_info = result
            
            if product_info:
                # Convertir el formato de extract_product_details al formato esperado por el resto del código
                formatted_info = {
                    'url': product_info.get('url', url),
                    'category': product_info.get('category', category_name),
                    'subcategory': product_info.get('subcategory', subcategory_name),
                    'title': product_info.get('nombre_producto', ''),
                    'price': product_info.get('precio_oferta', product_info.get('precio_regular', '')),
                    'old_price': product_info.get('precio_regular') if product_info.get('precio_oferta') else None,
                    'description': product_info.get('description', ''),
                    'specs': {},
                    'sizes': product_info.get('talles_todos', []),
                    'available_sizes': product_info.get('talles_disponibles', []),
                    'colors': product_info.get('colors', []),
                    'image_urls': product_info.get('image_urls', []),
                    'sku': product_info.get('sku', ''),
                    'availability': product_info.get('availability', False)
                }
                
                # Validar datos del producto antes de agregar
                if not self.validate_product_data(formatted_info):
                    logger.warning(f"Skipping invalid product: {url}")
                    failed += 1
                    continue
                
                # Almacena el producto si la extracción fue exitosa
                category_key = f"{category_name}_{subcategory_name}"
                self.products_by_category[category_key].append(formatted_info)
                successful += 1
                
                if formatted_info.get('available_sizes'):
                    print(f"✅ Product saved: {formatted_info.get('title', 'Unknown')} - Talles disponibles: {formatted_info.get('available_sizes', [])}")
                else:
                    print(f"⚠️  Product saved (no sizes): {formatted_info.get('title', 'Unknown')}")
            else:
                failed += 1
                logger.warning(f"Failed to extract product info for {url}")
        
        logger.info(f"Processing complete: {successful} successful, {failed} failed")
    
    def is_element_disabled(self, element) -> bool:
        """
        Verifica si un elemento está deshabilitado
        """
        if not element:
            return True
        
        # Verificar atributo disabled
        if element.get('disabled'):
            return True
        
        # Verificar clases que indiquen deshabilitado
        disabled_classes = ['disabled', 'unavailable', 'out-of-stock', 'no-stock']
        element_classes = element.get('class', [])
        if isinstance(element_classes, str):
            element_classes = [element_classes]
        
        for disabled_class in disabled_classes:
            if disabled_class in element_classes:
                return True
        
        # Verificar atributos data que indiquen deshabilitado
        if element.get('data-option-empty') == 'true':
            return True
        
        # Verificar tabindex negativo (elemento no interactivo)
        tabindex = element.get('tabindex')
        if tabindex and tabindex == '-1':
            return True
        
        # Verificar estilo que indique deshabilitado
        style = element.get('style', '').lower()
        if 'opacity: 0.5' in style or 'opacity:0.5' in style or 'opacity: 0.3' in style:
            return True
        
        return False
    

    
    def clean_size_text(self, size_text: str) -> str:
        """
        Limpia y normaliza el texto del talle
        Optimizado para el formato de Rapsodia que incluye números de talles
        """
        if not size_text:
            return ""
        
        # Limpiar espacios y caracteres especiales
        cleaned = size_text.strip()
        
        # Para el formato de Rapsodia, manejar talles como "38/XS", "40/S", etc.
        if '/' in cleaned:
            # Separar el número del talle y la letra
            parts = cleaned.split('/')
            if len(parts) == 2:
                number_part = parts[0].strip()
                letter_part = parts[1].strip()
                
                # Normalizar la parte de la letra
                normalized_letter = self.normalize_size_letter(letter_part)
                
                # Retornar en formato "NÚMERO/LETRA" (ej: "38/XS")
                return f"{number_part}/{normalized_letter}"
        
        # Normalizar talles comunes
        size_mapping = {
            'XS': 'XS', 'xs': 'XS',
            'S': 'S', 's': 'S',
            'M': 'M', 'm': 'M',
            'L': 'L', 'l': 'L',
            'XL': 'XL', 'xl': 'XL',
            'XXL': 'XXL', 'xxl': 'XXL',
            'XXXL': 'XXXL', 'xxxl': 'XXXL',
            'P': 'P', 'p': 'P',
            'G': 'G', 'g': 'G',
            'GG': 'GG', 'gg': 'GG',
            'XG': 'XG', 'xg': 'XG'
        }
        
        # Intentar mapear el talle
        if cleaned in size_mapping:
            return size_mapping[cleaned]
        
        # Si no está en el mapeo, retornar el texto original en mayúsculas
        return cleaned.upper()
    
    def normalize_size_letter(self, letter: str) -> str:
        """
        Normaliza la letra del talle (S, M, L, etc.)
        """
        letter = letter.strip().upper()
        
        # Mapeo de letras de talles
        letter_mapping = {
            'XS': 'XS',
            'S': 'S',
            'M': 'M',
            'L': 'L',
            'XL': 'XL',
            'XXL': 'XXL',
            'XXXL': 'XXXL',
            'P': 'P',
            'G': 'G',
            'GG': 'GG',
            'XG': 'XG'
        }
        
        return letter_mapping.get(letter, letter)
    

    
    def validate_image_url(self, url: str) -> bool:
        """
        Valida que una URL de imagen sea válida y específicamente de productos de Rapsodia
        """
        if not url:
            return False
        
        # Verificar que empiece con https
        if not url.startswith('https://'):
            return False
        
        # Verificar que sea específicamente de la galería de productos de Rapsodia
        if not url.startswith('https://grupo-alas.com.ar/media/catalog/product/'):
            return False
        
        # Verificar que sea una imagen con extensión válida
        image_extensions = ['.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp']
        url_lower = url.lower()
        
        # Verificar extensión de imagen
        has_image_extension = any(ext in url_lower for ext in image_extensions)
        if not has_image_extension:
            return False
        
        # Verificar que no sea una imagen placeholder o de error
        invalid_patterns = [
            'placeholder', 'no-image', 'error', 'default', 'blank',
            'missing', 'unavailable', 'not-found', 'spacer', 'transparent',
            'logo', 'icon', 'banner', 'header', 'footer', 'nav'
        ]
        
        for pattern in invalid_patterns:
            if pattern in url_lower:
                return False
        
        return True
    
    def clean_image_url(self, url: str) -> str:
        """
        Limpia una URL de imagen removiendo solo parámetros muy específicos
        Mantiene la unicidad de las URLs para evitar duplicados
        """
        if not url:
            return ""
        
        # Solo remover parámetros muy específicos que no afecten la unicidad
        # Mantener la URL lo más original posible
        cleaned_url = url
        
        # Remover solo parámetros de cache muy específicos que no afecten la imagen
        cache_params = [
            '?v=', '&v=', '?version=', '&version=', '?cache=', '&cache=',
            '?t=', '&t=', '?timestamp=', '&timestamp='
        ]
        
        for param in cache_params:
            if param in cleaned_url:
                param_start = cleaned_url.find(param)
                if param_start != -1:
                    # Encontrar el final del parámetro (siguiente & o fin de URL)
                    param_end = cleaned_url.find('&', param_start + 1)
                    if param_end == -1:
                        param_end = len(cleaned_url)
                    
                    # Remover el parámetro completo
                    cleaned_url = cleaned_url[:param_start] + cleaned_url[param_end:]
        
        # Limpiar URLs que terminan en ? o &
        cleaned_url = cleaned_url.rstrip('?&')
        
        # Si la URL limpia está vacía, devolver la original
        if not cleaned_url:
            return url
        
        return cleaned_url
    
    def clean_description_text(self, element) -> str:
        """
        Limpia el texto de descripción del producto, removiendo elementos innecesarios
        """
        if not element:
            return ""
        
        # Remover elementos que no queremos en la descripción
        unwanted_selectors = [
            "ul.data.additional-attributes",  # Tabla de especificaciones
            ".data.additional-attributes",    # Especificaciones adicionales
            "#product-attribute-specs-table", # Tabla de specs
            ".product-attribute-specs-table", # Tabla de specs
            "li",                            # Elementos de lista de specs
            "h5.label",                      # Labels de especificaciones
            "p.data[data-th]"                # Datos de especificaciones
        ]
        
        # Crear una copia del elemento para no modificar el original
        element_copy = element.copy()
        
        # Remover elementos no deseados
        for selector in unwanted_selectors:
            for unwanted in element_copy.select(selector):
                unwanted.decompose()
        
        # Obtener el texto limpio
        clean_text = element_copy.get_text().strip()
        
        # Limpiar espacios extra y saltos de línea
        clean_text = ' '.join(clean_text.split())
        
        return clean_text
    
    async def fetch(self, session, url: str) -> str:
        """
        Función para obtener HTML de una URL usando aiohttp
        """
        try:
            # Configurar SSL context para evitar errores de certificado
            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            
            async with aiohttp.ClientSession(connector=connector) as temp_session:
                async with temp_session.get(url, timeout=30) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")
                        return ""
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return ""

    async def start_crawling(self):
        """
        Inicia el proceso completo de crawling
        """
        print("=== Starting Safe Rapsodia Scraper ===")
        print("🎯 El scraper detectará automáticamente cuando una categoría esté completa")
        
        # Inicializar tracking para categorías principales
        self.category_progress['WOMAN'] = {
            'started': True,
            'pages_processed': 0,
            'products_found': 0
        }
        self.category_progress['GIRLS'] = {
            'started': True,
            'pages_processed': 0,
            'products_found': 0
        }
        self.category_progress['DENIM'] = {
            'started': True,
            'pages_processed': 0,
            'products_found': 0
        }
        
        # URLs específicas para WOMAN subcategorías (en lugar de la página principal)
        woman_subcategories = [
            "https://www.rapsodia.com.ar/woman/jean.html",
            "https://www.rapsodia.com.ar/woman/camisas-y-tops.html",
            "https://www.rapsodia.com.ar/woman/remeras.html",
            "https://www.rapsodia.com.ar/woman/vestidos.html",
            "https://www.rapsodia.com.ar/woman/camperas-kimonos.html",
            "https://www.rapsodia.com.ar/woman/pantalones.html",
            "https://www.rapsodia.com.ar/woman/buzos-y-sweaters.html",
            "https://www.rapsodia.com.ar/woman/polleras-y-shorts.html",
            "https://www.rapsodia.com.ar/woman/cueros.html",
            "https://www.rapsodia.com.ar/woman/calzado-mujer.html",
            "https://www.rapsodia.com.ar/woman/carteras-y-bolsos.html",
            "https://www.rapsodia.com.ar/woman/pa-uelos-y-accesorios.html",
            "https://www.rapsodia.com.ar/woman/bijou.html",
            "https://www.rapsodia.com.ar/woman/perfumes.html"
        ]
        
        # Procesar WOMAN subcategorías directamente
        print(f"\n{'='*50}")
        print(f"Processing WOMAN subcategories")
        print(f"{'='*50}")
        
        for subcategory_url in woman_subcategories:
            print(f"\nProcessing WOMAN subcategory: {subcategory_url}")
            subcategory_name = self.get_subcategory_name_from_url(subcategory_url)
            print(f"Subcategory: {subcategory_name}")
            
            # Procesar directamente la subcategoría
            await self.process_subcategory_with_pagination(subcategory_url, max_pages=3, parent_category="WOMAN", subcategory_name=subcategory_name)
            
            # Pausa entre subcategorías
            await asyncio.sleep(1)
        
        # URLs específicas para GIRLS subcategorías
        girls_subcategories = [
            "https://www.rapsodia.com.ar/girls/jeans.html",
            "https://www.rapsodia.com.ar/girls/remeras-y-camisas.html",
            "https://www.rapsodia.com.ar/girls/camperas-y-sacos.html",
            "https://www.rapsodia.com.ar/girls/pantalones.html",
            "https://www.rapsodia.com.ar/girls/buzos-y-sweaters.html",
            "https://www.rapsodia.com.ar/girls/calzado-nina.html"
        ]
        
        # Procesar GIRLS subcategorías directamente
        print(f"\n{'='*50}")
        print(f"Processing GIRLS subcategories")
        print(f"{'='*50}")
        
        for subcategory_url in girls_subcategories:
            print(f"\nProcessing GIRLS subcategory: {subcategory_url}")
            subcategory_name = self.get_subcategory_name_from_url(subcategory_url)
            print(f"Subcategory: {subcategory_name}")
            
            # Procesar directamente la subcategoría
            await self.process_subcategory_with_pagination(subcategory_url, max_pages=3, parent_category="GIRLS", subcategory_name=subcategory_name)
            
            # Pausa entre subcategorías
            await asyncio.sleep(1)
        
        # URLs específicas para DENIM subcategorías
        denim_subcategories = [
            "https://www.rapsodia.com.ar/denim/jeans.html",
            "https://www.rapsodia.com.ar/denim/camperas.html",
            "https://www.rapsodia.com.ar/denim/faldas.html",
            "https://www.rapsodia.com.ar/denim/chalecos.html"
        ]
        
        # Procesar DENIM subcategorías directamente
        print(f"\n{'='*50}")
        print(f"Processing DENIM subcategories")
        print(f"{'='*50}")
        
        for subcategory_url in denim_subcategories:
            print(f"\nProcessing DENIM subcategory: {subcategory_url}")
            subcategory_name = self.get_subcategory_name_from_url(subcategory_url)
            print(f"Subcategory: {subcategory_name}")
            
            # Verificar si la URL existe antes de procesar
            try:
                response = requests.head(subcategory_url, timeout=5)
                if response.status_code == 200:
                    print(f"  ✓ URL válida: {subcategory_url}")
                    # Procesar directamente la subcategoría
                    await self.process_subcategory_with_pagination(subcategory_url, max_pages=3, parent_category="DENIM", subcategory_name=subcategory_name)
                else:
                    print(f"  ✗ URL no válida (status: {response.status_code}): {subcategory_url}")
            except Exception as e:
                print(f"  ✗ Error verificando URL: {subcategory_url} - {e}")
            
            # Pausa entre subcategorías
            await asyncio.sleep(1)
        
        # URLs para otras categorías principales
        other_categories = [
            "https://www.rapsodia.com.ar/sale/",
            "https://www.rapsodia.com.ar/home/",
            "https://www.rapsodia.com.ar/vintage/"
        ]
        
        for url in other_categories:
            print(f"\n{'='*50}")
            print(f"Processing: {url}")
            print(f"{'='*50}")
            await self.crawl_with_pagination(url, max_pages=3)
            
            # Pausa entre categorías
            await asyncio.sleep(2)
        
        # Generar markdowns organizados
        print("\n=== Generating Organized Markdowns ===")
        self.save_all_data()

async def main():
    scraper = RapsodiaSafeScraper()
    await scraper.start_crawling()

if __name__ == "__main__":
    asyncio.run(main()) 