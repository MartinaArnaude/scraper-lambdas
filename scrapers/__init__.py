"""
Scrapers Module - Módulo base para todos los scrapers
"""

from .base_scraper import BaseScraper
from .supabase_client import SupabaseClient

__all__ = ['BaseScraper', 'SupabaseClient']

