"""
Script de parsing HDFS en mode streaming (économe en mémoire).
"""
import os
import sys
import traceback
import shutil
import datetime

sys.path.insert(0, '/app/parser')

from cache_manager import CacheManager
from hdfs.hdfs_processor import HDFSLogProcessor


# Configuration
INPUT_DIR = '/data/hdfs/raw/'
OUTPUT_DIR = '/data/hdfs/parsed/'
STATE_DIR = '/data/hdfs/state/'
ARCHIVE_DIR = '/data/hdfs/archive/'
LOG_FILE_NAME = 'HDFS.log'

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def main():
    """Fonction principale."""
    
    try:
        print("="*80)
        print("PARSING HDFS EN MODE STREAMING")
        print("="*80)
        
        # Cache
        cache_manager = CacheManager(STATE_DIR)
        needs_parsing, reason = cache_manager.check_cache(
            LOG_FILE_NAME, INPUT_DIR, OUTPUT_DIR
        )
        
        if not needs_parsing:
            print(f"\n {reason}")
            print(f" Fichiers disponibles dans: {OUTPUT_DIR}")
            
            metadata = cache_manager.load_metadata()
            if LOG_FILE_NAME in metadata:
                info = metadata[LOG_FILE_NAME]
                print(f"\n Statistiques:")
                print(f"  - Lignes: {info['num_lines']:,}")
                print(f"  - Templates: {info['num_templates']}")
            
            print(f"\n Pour forcer: rm {STATE_DIR}/parsing_metadata.json")
            print("="*80)
            return
        
        print(f"\n {reason}")
        print(f" Fichier: {LOG_FILE_NAME}\n")
        
        file_path = os.path.join(INPUT_DIR, LOG_FILE_NAME)
        
        if not os.path.exists(file_path):
            print(f" ERREUR: {file_path} introuvable")
            print(f"   Contenu: {os.listdir(INPUT_DIR) if os.path.exists(INPUT_DIR) else 'N/A'}")
            return
        
        # Taille du fichier
        size_mb = os.path.getsize(file_path) / 1024 / 1024
        print(f" Taille: {size_mb:.2f} MB")
        
        # Initialiser le processeur
        processor = HDFSLogProcessor(config_file='drain.ini')
        
        # Chemins de sortie
        structured_path = os.path.join(OUTPUT_DIR, 'HDFS_structured.csv')
        templates_path = os.path.join(OUTPUT_DIR, 'HDFS_templates.csv')
        
        # PARSING EN STREAMING (économe en mémoire)
        print(f"\n Mode streaming activé (sauvegarde par batch de 100k)\n")
        
        total_lines = processor.parse_and_save_streaming(
            file_path=file_path,
            output_path=structured_path,
            batch_size=100000,      # Sauvegarder tous les 100k
            progress_interval=50000  # Afficher tous les 50k
        )
        
        # Créer le fichier templates
        print(f"\n Sauvegarde des templates...")
        df_templates = processor.create_templates_dataframe()
        df_templates.to_csv(templates_path, index=False)
        
        print(f" HDFS_structured.csv")
        print(f" HDFS_templates.csv")
        
        # Statistiques
        processor.get_statistics_with_blockids(total_lines, df_templates, structured_path)
        
        # Cache
        stats = {'num_lines': total_lines, 'num_templates': len(df_templates)}
        cache_manager.update_cache(LOG_FILE_NAME, file_path, stats)

        # Archiver le fichier source après un parsing reussi
        archive_name = f"HDFS_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        archive_path = os.path.join(ARCHIVE_DIR, archive_name)
        try:
            shutil.move(file_path, archive_path)
            print(f"\n  Fichier archive: {archive_path}")
        except Exception:
            print(f"\n echec de l'archivage:\n{traceback.format_exc()}")
        
        print(f"\n   ✓ Cache mis à jour")
        print("\n" + "="*80)
        print("Parsing HDFS terminé avec succès!")
        print("="*80)
        
    except Exception as e:
        print(f"\n Erreur :")
        print(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()