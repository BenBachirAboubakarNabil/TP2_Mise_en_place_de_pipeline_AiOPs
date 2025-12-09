import pandas as pd
import numpy as np
import os
from collections import Counter, defaultdict


# Configuration
PARSED_DIR = '/data/hdfs/parsed/'
OUTPUT_DIR = '/data/hdfs/vectorized/'
LABELS_FILE = '/data/hdfs/raw/anomaly_label.csv'

os.makedirs(OUTPUT_DIR, exist_ok=True)


def charger_donnees():
    
    structured_path = os.path.join(PARSED_DIR, 'HDFS_structured.csv')
    templates_path = os.path.join(PARSED_DIR, 'HDFS_templates.csv')
    
    if not os.path.exists(structured_path):
        print(f"\n Fichier introuvable: {structured_path}")
        return None, None
    
    # Charger templates (petit fichier)
    df_templates = pd.read_csv(templates_path)

    return structured_path, df_templates


def vectoriser_par_blockid_streaming(structured_path, all_event_ids):
    
    # Dictionnaire pour stocker les comptages
    block_events = defaultdict(Counter)
    
    # Lire le CSV en chunks pour économiser la mémoire
    chunk_size = 500000
    total_lines = 0
    
    for chunk in pd.read_csv(structured_path, chunksize=chunk_size):
        # Filtrer les lignes avec BlockID
        chunk_with_blocks = chunk[chunk['BlockId'].notna()]
        
        # Grouper par BlockID et compter les événements
        for block_id, group in chunk_with_blocks.groupby('BlockId'):
            event_counts = group['EventId'].value_counts()
            block_events[block_id].update(event_counts.to_dict())
        
        total_lines += len(chunk)
        
        if total_lines % 1000000 == 0:
            print(f"  Traité {total_lines:,} lignes...", flush=True)
    
    print(f"   ✓ {total_lines:,} lignes traitées")
    print(f"   ✓ {len(block_events):,} BlockIDs uniques trouvés")
    
    return block_events


def creer_matrice(block_events, all_event_ids):
    
    # Trier les EventIds (E1, E2, E3, ...)
    event_ids_sorted = sorted(all_event_ids, key=lambda x: int(x[1:]))
    
    # Créer la matrice
    rows = []
    
    for block_id in sorted(block_events.keys()):
        row = {'BlockId': block_id}
        
        # Pour chaque événement, ajouter le comptage (0 si absent)
        for event_id in event_ids_sorted:
            row[event_id] = block_events[block_id].get(event_id, 0)
        
        rows.append(row)
        
        if len(rows) % 50000 == 0:
            print(f"   Créé {len(rows):,} lignes...", flush=True)
    
    df_matrix = pd.DataFrame(rows)
    
    # S'assurer que toutes les colonnes d'événements sont présentes
    for event_id in event_ids_sorted:
        if event_id not in df_matrix.columns:
            df_matrix[event_id] = 0
    
    # Réordonner les colonnes
    columns_order = ['BlockId'] + event_ids_sorted
    df_matrix = df_matrix[columns_order]
    
    print(f"  Matrice créée: {len(df_matrix):,} BlockIDs × {len(event_ids_sorted)} événements")
    
    return df_matrix


def ajouter_labels(df_matrix):

    if not os.path.exists(LABELS_FILE):
        print(f"\n Fichier de labels non trouvé: {LABELS_FILE}")
        print(f" La matrice sera créée sans labels")
        return df_matrix
    
    df_labels = pd.read_csv(LABELS_FILE)
    
    # Fusionner
    df_matrix = df_matrix.merge(df_labels, on='BlockId', how='left')
    
    #df_matrix['Label'].fillna("Unknown", inplace=True)
    
    return df_matrix


def statistiques_matrice(df_matrix):
    print(f"\n STATISTIQUES DE LA MATRICE:")
    print(f"   {'='*70}")
    
    event_cols = [col for col in df_matrix.columns if col.startswith('E')]
    
    print(f"   Dimensions: {len(df_matrix):,} BlockIDs × {len(event_cols)} événements")
    
    # Densité
    total_cells = len(df_matrix) * len(event_cols)
    non_zero = (df_matrix[event_cols] > 0).sum().sum()
    density = (non_zero / total_cells) * 100
    
    print(f"   Densité: {density:.2f}% (cellules non-nulles)")
    
    # Statistiques par BlockID
    df_matrix['TotalEvents'] = df_matrix[event_cols].sum(axis=1)
    
    print(f"\n   Événements par BlockID:")
    print(f"      Moyenne: {df_matrix['TotalEvents'].mean():.2f}")
    print(f"      Min:     {df_matrix['TotalEvents'].min()}")
    print(f"      Max:     {df_matrix['TotalEvents'].max()}")
    print(f"      Médiane: {df_matrix['TotalEvents'].median():.0f}")
    
    # Top événements
    event_totals = df_matrix[event_cols].sum().sort_values(ascending=False)
    
    print(f"\n   🔝 Top 10 des événements les plus fréquents:")
    for i, (event_id, count) in enumerate(event_totals.head(10).items(), 1):
        pct = count / event_totals.sum() * 100
        print(f"      {i:>2}. {event_id}: {int(count):>8,} ({pct:>5.2f}%)")


def main():
    """Fonction principale."""
    
    # Charger les données
    structured_path, df_templates = charger_donnees()
    
    if structured_path is None:
        return
    
    # Liste de tous les EventIds
    all_event_ids = df_templates['EventId'].tolist()
    
    # Vectoriser par BlockID (streaming)
    block_events = vectoriser_par_blockid_streaming(structured_path, all_event_ids)
    
    # Créer la matrice
    df_matrix = creer_matrice(block_events, all_event_ids)
    
    # Ajouter les labels
    df_matrix = ajouter_labels(df_matrix)
    
    # Statistiques
    statistiques_matrice(df_matrix)
    
    # Sauvegarder
    output_path = os.path.join(OUTPUT_DIR, 'HDFS_event_occurrence_matrix.csv')
    df_matrix.to_csv(output_path, index=False)
    
    print(f"   ✓ {os.path.basename(output_path)}")
    

if __name__ == "__main__":
    main()