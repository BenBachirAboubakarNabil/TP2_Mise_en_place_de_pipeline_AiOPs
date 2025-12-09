import os
import json
import hashlib
from datetime import datetime


class CacheManager:
    
    def __init__(self, state_dir):
        """
        Initialise le gestionnaire de cache.
        
        Args:
            state_dir: Dossier pour stocker les métadonnées de cache
        """
        self.state_dir = state_dir
        self.metadata_file = os.path.join(state_dir, 'parsing_metadata.json')
        os.makedirs(state_dir, exist_ok=True)
    
    def get_file_hash(self, file_path):
        """
        Calcule le hash MD5 d'un fichier.
        """
        if not os.path.exists(file_path):
            return None
        
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def load_metadata(self):
        """Charge les métadonnées du cache"""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}
    
    def save_metadata(self, metadata):
        """Sauvegarde les métadonnées du cache"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata, indent=2, fp=f)
        except IOError as e:
            print(f"⚠️ Erreur sauvegarde cache: {e}")
    
    def check_cache(self, log_file_name, input_dir, output_dir):
        """
        Vérifie si le parsing est nécessaire.
        """
        input_path = os.path.join(input_dir, log_file_name)
        metadata = self.load_metadata()
        
        #  Fichier absent dans /raw/
        if not os.path.exists(input_path):
            if log_file_name in metadata:
                return False, "✓ Fichier déjà parsé (archivé), résultats en cache"
            else:
                return True, "⚠️ Aucun fichier à parser dans /raw/"
        
        #  Fichier présent → vérifier hash
        current_hash = self.get_file_hash(input_path)
        
        if log_file_name in metadata:
            cached_hash = metadata[log_file_name].get('file_hash')
            
            if cached_hash == current_hash:
                #  Même fichier 
                return False, " Fichier identique déjà parsé (hash identique), skip"
            else:
                #  Fichier modifié
                return True, " Nouveau fichier détecté (hash différent), parsing nécessaire"
        
        #  Première fois
        return True, " Nouveau fichier, parsing nécessaire"
    
    def update_cache(self, log_file_name, input_path, stats):
        """
        Met à jour le cache après un parsing réussi.
        IMPORTANT: Appeler AVANT de move/archiver le fichier.
        """
        metadata = self.load_metadata()
        
        # Calculer hash AVANT que le fichier soit déplacé
        file_hash = self.get_file_hash(input_path)
        
        metadata[log_file_name] = {
            'file_hash': file_hash,
            'last_parsed': datetime.now().isoformat(),
            'num_lines': stats.get('num_lines', 0),
            'num_templates': stats.get('num_templates', 0)
        }
        
        self.save_metadata(metadata)
    
    def get_cache_info(self, log_file_name):
        """Récupère les informations de cache pour un fichier"""
        metadata = self.load_metadata()
        return metadata.get(log_file_name)
    
    def clear_cache(self, log_file_name=None):
        """Vide le cache (tout ou pour un fichier spécifique)"""
        if log_file_name is None:
            if os.path.exists(self.metadata_file):
                os.remove(self.metadata_file)
                print("✓ Cache vidé complètement")
        else:
            metadata = self.load_metadata()
            if log_file_name in metadata:
                del metadata[log_file_name]
                self.save_metadata(metadata)
                print(f"✓ Cache vidé pour {log_file_name}")