import re
import sys
sys.path.insert(0, '/app/parser')

from log_processor import LogProcessor


class OpenStackLogProcessor(LogProcessor):
    """Processeur pour les logs OpenStack."""
    
    # Pattern: filename timestamp PID LEVEL component [req-id] message
    LOG_PATTERN = re.compile(
        r'^([\w\-\.]+)\s+'                          # Filename
        r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)\s+'  # Timestamp
        r'(\d+)\s+'                                 # PID
        r'(\w+)\s+'                                 # Level
        r'([\w\.]+)\s+'                            # Component
        r'(\[.*?\])?\s*'                           # RequestId (optionnel)
        r'(.*)$'                                   # Content
    )
    
    # Pattern pour extraire InstanceId
    INSTANCE_ID_PATTERN = re.compile(r'\[instance:\s+([\w\-]+)\]')
    
    def get_log_pattern(self):
        return self.LOG_PATTERN
    
    def extract_fields(self, line, match):
        """Extrait les champs d'une ligne parsée."""
        filename, timestamp, pid, level, component, request_id, content = match.groups()
        
        # Extraire InstanceId si présent
        instance_match = self.INSTANCE_ID_PATTERN.search(content)
        instance_id = instance_match.group(1) if instance_match else None
        
        return {
            'Filename': filename,
            'Timestamp': timestamp,
            'Pid': pid,
            'Level': level,
            'Component': component,
            'RequestId': request_id if request_id else '',
            'Content': content,
            'InstanceId': instance_id
        }
    
    def create_unparsed_entry(self, line, line_id):
        """Crée une entrée pour ligne non parsée."""
        # Tenter d'extraire InstanceId même si ligne mal formée
        instance_match = self.INSTANCE_ID_PATTERN.search(line)
        instance_id = instance_match.group(1) if instance_match else None
        
        return {
            'LineId': line_id,
            'Filename': '',
            'Timestamp': '',
            'Pid': '',
            'Level': '',
            'Component': '',
            'RequestId': '',
            'Content': line,
            'InstanceId': instance_id
        }
    
    def get_column_order(self):
        return [
            'LineId', 'Filename', 'Timestamp', 'Pid', 'Level', 'Component',
            'RequestId', 'Content', 'InstanceId', 'EventId', 'EventTemplate'
        ]
    
    def get_statistics_with_instances(self, total_lines, df_templates, structured_path):
        
        # Stats de base
        self.get_statistics(total_lines, df_templates)
        
        instance_count = 0
        unique_instances = set()
        
        import csv
        with open(structured_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['InstanceId']:
                    instance_count += 1
                    unique_instances.add(row['InstanceId'])
        
        print(f"  - Lignes avec InstanceID: {instance_count:,}")
        print(f"  - InstanceIDs uniques: {len(unique_instances):,}")