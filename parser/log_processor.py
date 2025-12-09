"""
Module de base pour le parsing de logs avec streaming.
Ne charge JAMAIS tout en mémoire.
"""
import os
import re
import pandas as pd
from abc import ABC, abstractmethod
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig


class LogProcessor(ABC):
    """Classe de base pour le parsing de logs avec mode streaming."""
    
    def __init__(self, config_file='drain.ini'):
        self.config_file = config_file
        if not os.path.isabs(self.config_file) and not os.path.exists(self.config_file):
            module_dir = os.path.dirname(__file__)
            candidate = os.path.join(module_dir, self.config_file)
            if os.path.exists(candidate):
                self.config_file = candidate
            else:
                pass
        self.template_miner = None
        
    @abstractmethod
    def get_log_pattern(self):
        """Retourne le pattern regex."""
        pass
    
    @abstractmethod
    def extract_fields(self, line, match):
        """Extrait les champs d'une ligne parsée."""
        pass
    
    @abstractmethod
    def create_unparsed_entry(self, line, line_id):
        """Crée une entrée pour ligne non parsée."""
        pass
    
    @abstractmethod
    def get_column_order(self):
        """Retourne l'ordre des colonnes pour le CSV."""
        pass
    
    def parse_and_save_streaming(self, file_path, output_path, 
                                  batch_size=100000, progress_interval=50000):
        """
        Parse le fichier en streaming et sauvegarde par batch.
        NE CHARGE JAMAIS TOUT EN MÉMOIRE.
        
        Args:
            file_path: Fichier d'entrée
            output_path: Fichier de sortie CSV
            batch_size: Taille des batchs pour sauvegarde
            progress_interval: Intervalle d'affichage
        """
        print(f"📖 Parsing en mode streaming: {file_path}")
        
        # Initialiser Drain3
        config = TemplateMinerConfig()
        config.load(self.config_file)
        self.template_miner = TemplateMiner(config=config)
        
        pattern = self.get_log_pattern()
        batch = []
        total_lines = 0
        first_batch = True
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line_id, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                
                # Parser la ligne
                match = pattern.match(line)
                
                if match:
                    log_entry = self.extract_fields(line, match)
                    log_entry['LineId'] = line_id
                else:
                    log_entry = self.create_unparsed_entry(line, line_id)
                
                # Parser avec Drain3
                result = self.template_miner.add_log_message(log_entry['Content'])
                log_entry['EventId'] = f"E{result['cluster_id']}"
                log_entry['EventTemplate'] = result['template_mined']
                
                batch.append(log_entry)
                total_lines += 1
                
                # Sauvegarder le batch
                if len(batch) >= batch_size:
                    self._save_batch(batch, output_path, first_batch)
                    batch = []
                    first_batch = False
                
                # Afficher progression
                if line_id % progress_interval == 0:
                    print(f"   Traité {line_id:,} lignes...", flush=True)
        
        # Sauvegarder le dernier batch
        if batch:
            self._save_batch(batch, output_path, first_batch)
        
        print(f"   ✓ {total_lines:,} lignes parsées et sauvegardées")
        
        return total_lines
    
    def _save_batch(self, batch, output_path, is_first):
        """
        Sauvegarde un batch dans le CSV.
        
        Args:
            batch: Liste de dictionnaires
            output_path: Chemin du CSV
            is_first: True si c'est le premier batch (écrire header)
        """
        df = pd.DataFrame(batch)
        df = df[self.get_column_order()]
        
        # Append au CSV (header seulement si premier batch)
        df.to_csv(output_path, mode='a' if not is_first else 'w', 
                  header=is_first, index=False)
    
    def create_templates_dataframe(self):
        """Crée le DataFrame des templates."""
        template_dict = {}
        
        for cluster in self.template_miner.drain.clusters:
            event_id = f"E{cluster.cluster_id}"
            template_dict[event_id] = {
                'EventId': event_id,
                'EventTemplate': cluster.get_template(),
                'Occurrences': cluster.size
            }
        
        template_list = sorted(template_dict.values(), 
                              key=lambda x: int(x['EventId'][1:]))
        
        return pd.DataFrame(template_list)
    
    def get_statistics(self, total_lines, df_templates):
        """Affiche les statistiques."""
        print(f"\n📊 STATISTIQUES:")
        print(f"   - Total de lignes: {total_lines:,}")
        print(f"   - Templates uniques: {len(df_templates)}")
        
        print(f"\n🔝 Top 5 des templates les plus fréquents:")
        top = df_templates.nlargest(5, 'Occurrences')
        
        for _, row in top.iterrows():
            template = row['EventTemplate']
            if len(template) > 70:
                template = template[:70] + "..."
            print(f"   {row['EventId']}: {row['Occurrences']:>7,} fois - {template}")