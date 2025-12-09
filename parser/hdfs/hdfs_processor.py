"""
Processeur HDFS avec mode streaming.
"""
import re
import sys
sys.path.insert(0, '/app/parser')

from log_processor import LogProcessor


class HDFSLogProcessor(LogProcessor):
    """Processeur pour les logs HDFS."""
    
    LOG_PATTERN = re.compile(
        r'^(\d{6})\s+(\d{6})\s+(\d+)\s+(\w+)\s+([\w.$]+):\s+(.+)$'
    )
    
    BLOCK_ID_PATTERN = re.compile(r'(blk_-?\d+)')
    
    def get_log_pattern(self):
        return self.LOG_PATTERN
    
    def extract_fields(self, line, match):
        date, time, pid, level, component, content = match.groups()
        
        block_match = self.BLOCK_ID_PATTERN.search(content)
        block_id = block_match.group(1) if block_match else None
        
        return {
            'Date': date,
            'Time': time,
            'Pid': pid,
            'Level': level,
            'Component': component,
            'Content': content,
            'BlockId': block_id
        }
    
    def create_unparsed_entry(self, line, line_id):
        block_match = self.BLOCK_ID_PATTERN.search(line)
        block_id = block_match.group(1) if block_match else None
        
        return {
            'Date': '',
            'Time': '',
            'Pid': '',
            'Level': '',
            'Component': '',
            'Content': line,
            'BlockId': block_id
        }
    
    def get_column_order(self):
        """Ordre des colonnes pour HDFS."""
        return [
            'LineId', 'Date', 'Time', 'Pid', 'Level', 'Component',
            'Content', 'BlockId', 'EventId', 'EventTemplate'
        ]
    
    def get_statistics_with_blockids(self, total_lines, df_templates, structured_path):
        """Statistiques HDFS avec BlockIDs (sans charger tout le CSV)."""
        
        # Stats de base
        self.get_statistics(total_lines, df_templates)
        
        # Compter les BlockIDs en streaming
        print(f"\n   Calcul des statistiques BlockID...")
        
        block_count = 0
        unique_blocks = set()
        
        import csv
        with open(structured_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['BlockId']:
                    block_count += 1
                    unique_blocks.add(row['BlockId'])
        
        print(f"  - BlockIDs trouvés: {block_count:,}")
        print(f"  - BlockIDs uniques: {len(unique_blocks):,}")