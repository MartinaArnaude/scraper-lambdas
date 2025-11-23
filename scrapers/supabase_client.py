"""
Supabase Client - Maneja todas las operaciones de base de datos
Incluye lógica de detección de cambios y sincronización
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List, Optional, Set
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Cliente para interactuar con Supabase PostgreSQL"""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Inicializa el cliente de Supabase
        
        Args:
            connection_string: String de conexión a Supabase. Si es None, busca en variables de entorno
        """
        if connection_string:
            self.conn_string = connection_string
        else:
            # Buscar en variables de entorno (compatible con AWS Secrets Manager)
            self.conn_string = os.getenv(
                'SUPABASE_DB_URL',
                os.getenv('DATABASE_URL', '')
            )
        
        if not self.conn_string:
            raise ValueError("No se proporcionó connection_string y no se encontró en variables de entorno")
    
    def get_connection(self):
        """Obtiene una conexión a la base de datos"""
        return psycopg2.connect(self.conn_string)
    
    def get_brand_id(self, brand_name: str) -> Optional[str]:
        """
        Obtiene el ID de una marca, o la crea si no existe
        
        Args:
            brand_name: Nombre de la marca
            
        Returns:
            UUID de la marca
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Buscar marca existente
                cur.execute(
                    "SELECT brand_id FROM brands WHERE name = %s",
                    (brand_name,)
                )
                result = cur.fetchone()
                
                if result:
                    return result[0]
                
                # Crear nueva marca
                cur.execute(
                    """
                    INSERT INTO brands (name, created_at, updated_at)
                    VALUES (%s, NOW(), NOW())
                    RETURNING brand_id
                    """,
                    (brand_name,)
                )
                brand_id = cur.fetchone()[0]
                conn.commit()
                logger.info(f"Created new brand: {brand_name} with ID: {brand_id}")
                return brand_id
    
    def get_category_id(self, category_slug: str) -> Optional[str]:
        """Obtiene el ID de una categoría por slug"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM item_categories WHERE slug = %s",
                    (category_slug,)
                )
                result = cur.fetchone()
                return result[0] if result else None
    
    def get_subcategory_id(self, subcategory_slug: str, category_id: str) -> Optional[str]:
        """
        Obtiene el ID de una subcategoría, o la crea si no existe
        
        Args:
            subcategory_slug: Slug de la subcategoría
            category_id: ID de la categoría padre
            
        Returns:
            UUID de la subcategoría
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Buscar subcategoría existente
                cur.execute(
                    "SELECT id FROM item_subcategories WHERE slug = %s AND category_id = %s",
                    (subcategory_slug, category_id)
                )
                result = cur.fetchone()
                
                if result:
                    return result[0]
                
                # Crear nueva subcategoría (necesitarías el nombre también)
                # Por ahora retornamos None si no existe
                return None
    
    def get_color_id(self, color_name: str, hex_code: Optional[str] = None) -> Optional[str]:
        """
        Obtiene el ID de un color, o lo crea si no existe
        
        Args:
            color_name: Nombre del color
            hex_code: Código hexadecimal del color (opcional)
            
        Returns:
            UUID del color
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Buscar color existente
                cur.execute(
                    "SELECT color_id FROM item_colors WHERE name = %s",
                    (color_name.upper(),)
                )
                result = cur.fetchone()
                
                if result:
                    return result[0]
                
                # Crear nuevo color
                if not hex_code:
                    hex_code = self._color_name_to_hex(color_name)
                
                cur.execute(
                    """
                    INSERT INTO item_colors (name, hex_code, created_at)
                    VALUES (%s, %s, NOW())
                    RETURNING color_id
                    """,
                    (color_name.upper(), hex_code)
                )
                color_id = cur.fetchone()[0]
                conn.commit()
                logger.info(f"Created new color: {color_name} with ID: {color_id}")
                return color_id
    
    def _color_name_to_hex(self, color_name: str) -> str:
        """Convierte un nombre de color a hex (básico)"""
        color_map = {
            'NEGRO': '#000000',
            'BLANCO': '#FFFFFF',
            'AZUL': '#0000FF',
            'ROJO': '#FF0000',
            'VERDE': '#00FF00',
            'AMARILLO': '#FFFF00',
            'GRIS': '#808080',
            'ROSA': '#FFC0CB',
            'CRUDO': '#F5F5DC',
            'BEIGE': '#F5F5DC',
            'CAMEL': '#C19A6B',
            'CELESTE': '#87CEEB',
        }
        return color_map.get(color_name.upper(), '#CCCCCC')
    
    def upsert_item(self, item_data: Dict) -> Optional[str]:
        """
        Inserta o actualiza un item en la base de datos
        
        Args:
            item_data: Dict con los datos del item:
                - url: URL del producto (usado como identificador único)
                - name: Nombre del producto
                - description: Descripción
                - price: Precio
                - brand_id: ID de la marca
                - category_id: ID de la categoría principal
                - subcategory_id: ID de la subcategoría
                - sizes: Lista de talles disponibles
                - sizes_available: Lista de talles en stock
                - available: Boolean si está disponible
                
        Returns:
            UUID del item insertado/actualizado
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Buscar item existente por URL (asumiendo que guardamos URL en algún campo)
                # Por ahora usaremos el nombre como identificador único
                # TODO: Agregar campo url a la tabla items si no existe
                
                cur.execute(
                    """
                    SELECT item_id, available, sizes_available
                    FROM items
                    WHERE name = %s AND brand_id = %s
                    """,
                    (item_data['name'], item_data['brand_id'])
                )
                existing = cur.fetchone()
                
                if existing:
                    item_id, old_available, old_sizes_available = existing
                    
                    # Actualizar item existente
                    cur.execute(
                        """
                        UPDATE items
                        SET 
                            description = %s,
                            price = %s,
                            sizes = %s,
                            sizes_available = %s,
                            available = %s,
                            updated_at = NOW()
                        WHERE item_id = %s
                        RETURNING item_id
                        """,
                        (
                            item_data.get('description'),
                            item_data['price'],
                            item_data['sizes'],
                            item_data.get('sizes_available', []),
                            item_data.get('available', True),
                            item_id
                        )
                    )
                    conn.commit()
                    logger.info(f"Updated item: {item_data['name']} (ID: {item_id})")
                    
                    # Detectar cambios
                    if old_available and not item_data.get('available', True):
                        logger.warning(f"Item {item_data['name']} is now unavailable")
                    
                    if old_sizes_available != item_data.get('sizes_available', []):
                        logger.info(f"Item {item_data['name']} sizes changed: {old_sizes_available} -> {item_data.get('sizes_available', [])}")
                    
                    return item_id
                else:
                    # Insertar nuevo item
                    cur.execute(
                        """
                        INSERT INTO items (
                            name, description, price, brand_id,
                            main_category_id, subcategory_id,
                            sizes, sizes_available, available,
                            created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        RETURNING item_id
                        """,
                        (
                            item_data['name'],
                            item_data.get('description'),
                            item_data['price'],
                            item_data['brand_id'],
                            item_data.get('category_id'),
                            item_data.get('subcategory_id'),
                            item_data['sizes'],
                            item_data.get('sizes_available', []),
                            item_data.get('available', True)
                        )
                    )
                    item_id = cur.fetchone()[0]
                    conn.commit()
                    logger.info(f"Created new item: {item_data['name']} (ID: {item_id})")
                    return item_id
    
    def upsert_item_images(self, item_id: str, image_urls: List[str], color_id: Optional[str] = None):
        """
        Inserta o actualiza las imágenes de un item
        
        Args:
            item_id: UUID del item
            image_urls: Lista de URLs de imágenes
            color_id: ID del color asociado (opcional)
        """
        if not image_urls:
            return
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Eliminar imágenes existentes del item (o hacer upsert más inteligente)
                # Por ahora, eliminamos y recreamos
                cur.execute(
                    "DELETE FROM item_images WHERE item_id = %s",
                    (item_id,)
                )
                
                # Insertar nuevas imágenes
                image_data = []
                for idx, img_url in enumerate(image_urls):
                    image_data.append((
                        item_id,
                        img_url,
                        idx == 0,  # Primera imagen es primary
                        color_id
                    ))
                
                execute_values(
                    cur,
                    """
                    INSERT INTO item_images (item_id, url, is_primary, color_id, created_at)
                    VALUES %s
                    """,
                    image_data,
                    template="(%s, %s, %s, %s, NOW())"
                )
                conn.commit()
                logger.info(f"Inserted {len(image_urls)} images for item {item_id}")
    
    def link_item_colors(self, item_id: str, color_ids: List[str]):
        """
        Vincula un item con sus colores
        
        Args:
            item_id: UUID del item
            color_ids: Lista de IDs de colores
        """
        if not color_ids:
            return
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Eliminar relaciones existentes
                cur.execute(
                    "DELETE FROM item_x_item_colors WHERE item_id = %s",
                    (item_id,)
                )
                
                # Insertar nuevas relaciones
                color_data = [(item_id, color_id) for color_id in color_ids]
                execute_values(
                    cur,
                    """
                    INSERT INTO item_x_item_colors (item_id, color_id, created_at)
                    VALUES %s
                    ON CONFLICT (item_id, color_id) DO NOTHING
                    """,
                    color_data,
                    template="(%s, %s, NOW())"
                )
                conn.commit()
                logger.info(f"Linked {len(color_ids)} colors to item {item_id}")
    
    def mark_missing_items_unavailable(self, brand_id: str, found_skus: Set[str], run_date: datetime):
        """
        Marca como no disponibles los items que no aparecieron en el scraper
        
        Args:
            brand_id: ID de la marca
            found_skus: Set de SKUs encontrados en este run (usamos SKU como identificador único)
            run_date: Fecha del run actual
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Obtener todos los items activos de esta marca
                cur.execute(
                    """
                    SELECT item_id, name, sizes_available
                    FROM items
                    WHERE brand_id = %s AND available = true
                    """,
                    (brand_id,)
                )
                all_items = cur.fetchall()
                
                # Buscar items por SKU (asumiendo que el SKU está en el nombre o tenemos campo SKU)
                # Por ahora, comparamos por nombre que contiene el SKU
                items_to_mark_unavailable = []
                
                for item_id, name, sizes_available in all_items:
                    # Extraer SKU del nombre (formato típico: "SKU - Nombre" o similar)
                    # O buscar en una tabla de SKUs si existe
                    # Por ahora, marcamos como unavailable si no está en found_skus
                    # Esto requiere que guardemos SKUs en la BD
                    pass
                
                # Marcar items como no disponibles
                if items_to_mark_unavailable:
                    cur.execute(
                        """
                        UPDATE items
                        SET available = false, updated_at = NOW()
                        WHERE item_id = ANY(%s)
                        """,
                        (items_to_mark_unavailable,)
                    )
                    conn.commit()
                    logger.info(f"Marked {len(items_to_mark_unavailable)} items as unavailable")
    
    def sync_product_availability(self, brand_id: str, product_data_list: List[Dict]):
        """
        Sincroniza la disponibilidad de productos basándose en los datos del scraper
        
        Args:
            brand_id: ID de la marca
            product_data_list: Lista de dicts con datos de productos encontrados
                Cada dict debe tener: 'sku' o 'name', 'available', 'sizes_available'
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Obtener todos los items de la marca
                cur.execute(
                    """
                    SELECT item_id, name, available, sizes_available
                    FROM items
                    WHERE brand_id = %s
                    """,
                    (brand_id,)
                )
                existing_items = {row[1]: (row[0], row[2], row[3]) for row in cur.fetchall()}
                
                # Crear set de productos encontrados
                found_names = {p.get('name', '') for p in product_data_list if p.get('name')}
                
                # Marcar como no disponibles los que no aparecieron
                items_to_mark_unavailable = []
                for name, (item_id, current_available, current_sizes) in existing_items.items():
                    if name not in found_names and current_available:
                        items_to_mark_unavailable.append(item_id)
                
                if items_to_mark_unavailable:
                    cur.execute(
                        """
                        UPDATE items
                        SET available = false, updated_at = NOW()
                        WHERE item_id = ANY(%s)
                        """,
                        (items_to_mark_unavailable,)
                    )
                    logger.info(f"Marked {len(items_to_mark_unavailable)} items as unavailable (not found in scraper)")
                
                # Actualizar disponibilidad y talles de productos encontrados
                for product_data in product_data_list:
                    name = product_data.get('name')
                    if not name or name not in existing_items:
                        continue
                    
                    item_id, current_available, current_sizes = existing_items[name]
                    new_available = product_data.get('available', True)
                    new_sizes = product_data.get('sizes_available', [])
                    
                    # Detectar cambios
                    if current_available != new_available:
                        logger.info(f"Item {name} availability changed: {current_available} -> {new_available}")
                    
                    if set(current_sizes or []) != set(new_sizes):
                        logger.info(f"Item {name} sizes changed: {current_sizes} -> {new_sizes}")
                    
                    # Actualizar
                    cur.execute(
                        """
                        UPDATE items
                        SET available = %s, sizes_available = %s, updated_at = NOW()
                        WHERE item_id = %s
                        """,
                        (new_available, new_sizes, item_id)
                    )
                
                conn.commit()
                logger.info(f"Synced availability for {len(product_data_list)} products")
    
    def get_all_product_urls(self, brand_id: str) -> Set[str]:
        """
        Obtiene todas las URLs de productos de una marca
        
        Args:
            brand_id: ID de la marca
            
        Returns:
            Set de URLs de productos
        """
        # TODO: Implementar cuando tengamos campo url en items
        return set()

