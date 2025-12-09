"""
Script de parsing OpenStack en mode streaming.
Parser 3 fichiers: normal1, normal2, abnormal
"""
import os
import sys
import traceback
import shutil
import datetime

sys.path.insert(0, '/app/parser')

from cache_manager import CacheManager
from openstack.openstack_processor import OpenStackLogProcessor

# Configuration
INPUT_DIR = '/data/openstack/raw/'
OUTPUT_DIR = '/data/openstack/parsed/'
STATE_DIR = '/data/openstack/state/'
ARCHIVE_DIR = '/data/openstack/archive/'

LOG_FILES = [
    'openstack_normal1.log',
    'openstack_normal2.log',
    'openstack_abnormal.log'
]

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def parse_single_file(file_name, processor, cache_manager):
    """Parse un fichier de log."""
    
    print(f"\n{'='*80}")
    print(f" FICHIER: {file_name}")
    print(f"{'='*80}")
    
    needs_parsing, reason = cache_manager.check_cache(
        file_name, INPUT_DIR, OUTPUT_DIR
    )
    
    if not needs_parsing:
        metadata = cache_manager.load_metadata()
        if file_name in metadata:
            info = metadata[file_name]
            print(f"\n Statistiques:")
            print(f"  - Lignes: {info['num_lines']:,}")
            print(f"  - Templates: {info['num_templates']}")
        return None
    
    file_path = os.path.join(INPUT_DIR, file_name)
    
    if not os.path.exists(file_path):
        print(f" ERREUR: {file_path} introuvable")
        return None
    
    # Taille
    size_mb = os.path.getsize(file_path) / 1024 / 1024
    print(f" Taille: {size_mb:.2f} MB")
    
    # Output
    output_name = file_name.replace('.log', '_structured.csv')
    output_path = os.path.join(OUTPUT_DIR, output_name)
    
    total_lines = processor.parse_and_save_streaming(
        file_path=file_path,
        output_path=output_path,
        batch_size=50000,
        progress_interval=20000
    )
    
    print(f"\n {output_name}")
    
    # Cache
    stats = {'num_lines': total_lines, 'num_templates': len(processor.template_miner.drain.clusters)}
    cache_manager.update_cache(file_name, file_path, stats)
    
    # Archiver
    archive_name = f"{file_name}_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    archive_path = os.path.join(ARCHIVE_DIR, archive_name)
    try:
        shutil.move(file_path, archive_path)
        print(f" Fichier archivé: {archive_name}")
    except Exception:
        print(f"Echec archivage:\n{traceback.format_exc()}")
    
    print(f" Cache mis à jour")
    
    return total_lines

def main():
    """Fonction principale."""
    
    try:
        print("="*80)
        print("="*80)
        
        # Initialiser
        processor = OpenStackLogProcessor(config_file='drain.ini')
        cache_manager = CacheManager(STATE_DIR)
        
        total_all = 0
        parsed_files = []
        
        # Parser chaque fichier
        for file_name in LOG_FILES:
            lines = parse_single_file(file_name, processor, cache_manager)
            if lines:
                total_all += lines
                parsed_files.append(file_name)
        
        # Templates globaux
        templates_path = os.path.join(OUTPUT_DIR, 'OpenStack_templates.csv')
        
        if parsed_files: 
            print(f"\n{'='*80}")
            df_templates = processor.create_templates_dataframe()
            df_templates.to_csv(templates_path, index=False)
            print(f"OpenStack_templates.csv")
            
            print(f"\n{'='*80}")
            print("PARSING OPENSTACK TERMINÉ")
            print(f"{'='*80}")
            print(f"Fichiers parsés: {len(parsed_files)}/{len(LOG_FILES)}")
            for f in parsed_files:
                print(f" {f}")
            print(f"\nTotal lignes: {total_all:,}")
            print(f"Templates uniques: {len(df_templates)}")
            print("="*80)
        else:
            print(f"\n Tous les fichiers déjà parsés (cache)")
            print("="*80)
        
    except Exception as e:
        print(f"\n Erreur:")
        print(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()