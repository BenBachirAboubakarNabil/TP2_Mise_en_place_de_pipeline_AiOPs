
"""
Package de parsing de logs avec Drain3.

Modules:
- cache_manager: Gestion du cache pour éviter le reparsing
- log_processor: Traitement et parsing des logs
- parse_openssh: Script principal pour OpenSSH
- parse_linux: Script principal pour Linux 
"""

__version__ = '1.0.0'
__author__ = 'TP2 Machine Learning'

from .cache_manager import CacheManager
#from .log_processor import LogProcessor

__all__ = ['CacheManager']  # 'LogProcessor' removed from __all__