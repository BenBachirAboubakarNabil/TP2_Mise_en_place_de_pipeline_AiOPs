import pandas as pd
import numpy as np
import os
from collections import Counter, defaultdict


# Configuration
PARSED_DIR = '/data/openstack/parsed/'
OUTPUT_DIR = '/data/openstack/vectorized/'
RAW_DIR = '/data/openstack/raw/'

os.makedirs(OUTPUT_DIR, exist_ok=True)


PARSED_FILES = [
    ('openstack_normal1_structured.csv', 'Normal'),
    ('openstack_normal2_structured.csv', 'Normal'),
    ('openstack_abnormal_structured.csv', 'Anomaly')
]

# VM IDs avec anomalies (depuis anomaly_labels.txt)
ANOMALY_INSTANCES = {
    '544fd51c-4edc-4780-baae-ba1d80a0acfc',
    'ae651dff-c7ad-43d6-ac96-bbcd820ccca8',
    'a445709b-6ad0-40ec-8860-bec60b6ca0c2',
    '1643649d-2f42-4303-bfcd-7798baec19f9'
}


def charger_templates():
    """Charge les templates."""
    templates_path = os.path.join(PARSED_DIR, 'OpenStack_templates.csv')
    
    if not os.path.exists(templates_path):
        print(f"Templates introuvables: {templates_path}")
        return None
    
    df_templates = pd.read_csv(templates_path)
    print(f"{len(df_templates)} templates chargés")
    
    return df_templates


def vectoriser_par_instance_streaming(all_event_ids):
    print(f"\nVectorisation par InstanceId")
    
    instance_events = defaultdict(Counter)
    
    instance_labels = {}
    
    chunk_size = 100000
    total_lines = 0
    files_processed = 0
    
    # Traiter chaque fichier
    for filename, default_label in PARSED_FILES:
        filepath = os.path.join(PARSED_DIR, filename)
        
        if not os.path.exists(filepath):
            print(f"{filename} introuvable")
            continue
        
        print(f"\n  Traitement: {filename}")
        file_lines = 0
        
        for chunk in pd.read_csv(filepath, chunksize=chunk_size):
            chunk_with_instances = chunk[chunk['InstanceId'].notna()]
            
            for instance_id, group in chunk_with_instances.groupby('InstanceId'):
                event_counts = group['EventId'].value_counts()
                instance_events[instance_id].update(event_counts.to_dict())
                
                if instance_id in ANOMALY_INSTANCES:
                    instance_labels[instance_id] = 'Anomaly'
                elif instance_id not in instance_labels:
                    if default_label == 'Anomaly':
                        instance_labels[instance_id] = 'Anomaly'
                    else:
                        instance_labels[instance_id] = default_label
            
            file_lines += len(chunk)
            total_lines += len(chunk)
            
            if file_lines % 500000 == 0:
                print(f"    {file_lines:,} lignes...", flush=True)
        
        print(f"{file_lines:,} lignes traitées")
        files_processed += 1
    
    print(f"\n{total_lines:,} lignes totales")
    print(f"{len(instance_events):,} InstanceIDs uniques trouvés")
    print(f"{files_processed} fichiers traités")
    
    return instance_events, instance_labels


def creer_matrice(instance_events, instance_labels, all_event_ids):   
    event_ids_sorted = sorted(all_event_ids, key=lambda x: int(x[1:]))
    
    rows = []
    
    for instance_id in sorted(instance_events.keys()):
        row = {
            'InstanceId': instance_id,
            'Label': instance_labels.get(instance_id, 'Unknown')
        }
        
        for event_id in event_ids_sorted:
            row[event_id] = instance_events[instance_id].get(event_id, 0)
        
        rows.append(row)
        
        if len(rows) % 10000 == 0:
            print(f"  {len(rows):,} lignes...", flush=True)
    
    df_matrix = pd.DataFrame(rows)
    
    for event_id in event_ids_sorted:
        if event_id not in df_matrix.columns:
            df_matrix[event_id] = 0
    
    columns_order = ['InstanceId', 'Label'] + event_ids_sorted
    df_matrix = df_matrix[columns_order]
    
    return df_matrix


def statistiques_matrice(df_matrix):
    event_cols = [col for col in df_matrix.columns if col.startswith('E')]

    label_counts = df_matrix['Label'].value_counts()
    for label, count in label_counts.items():
        pct = count / len(df_matrix) * 100
        print(f"   {label}: {count:,} ({pct:.2f}%)")
    
    if 'Normal' in label_counts.index and 'Anomaly' in label_counts.index:
        ratio = label_counts['Normal'] / label_counts['Anomaly']
        print(f"      Ratio Normal:Anomaly = {ratio:.1f}:1")
    
    # Densité
    total_cells = len(df_matrix) * len(event_cols)
    non_zero = (df_matrix[event_cols] > 0).sum().sum()
    density = (non_zero / total_cells) * 100
    
    print(f"\n   Densité: {density:.2f}% (cellules non-nulles)")
    
    df_matrix['TotalEvents'] = df_matrix[event_cols].sum(axis=1)

    event_totals = df_matrix[event_cols].sum().sort_values(ascending=False)

    for i, (event_id, count) in enumerate(event_totals.head(10).items(), 1):
        pct = count / event_totals.sum() * 100
        print(f"      {i:>2}. {event_id}: {int(count):>8,} ({pct:>5.2f}%)")
    
    print(f"\n{'='*80}")


def main():

    df_templates = charger_templates()
    if df_templates is None:
        return
    
    # Liste EventIds
    all_event_ids = df_templates['EventId'].tolist()
    
    # Vectoriser par InstanceId (streaming)
    instance_events, instance_labels = vectoriser_par_instance_streaming(all_event_ids)
    
    if not instance_events:
        print("Aucune instance trouvée")
        return
    
    # Créer matrice
    df_matrix = creer_matrice(instance_events, instance_labels, all_event_ids)

    # Statistiques
    statistiques_matrice(df_matrix)
    
    # Sauvegarder
    output_path = os.path.join(OUTPUT_DIR, 'OpenStack_event_occurrence_matrix.csv')
    df_matrix.to_csv(output_path, index=False)
    
    print(f"\n Matrice sauvegardée:")
    print(f"   {output_path}")
    print("="*80)


if __name__ == "__main__":
    main()
    
  