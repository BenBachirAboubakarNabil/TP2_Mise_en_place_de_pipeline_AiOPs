import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Config
sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = (12, 6)

# Chemins
DATA_DIR = Path('/data/openstack')
VECTORIZED_DIR = DATA_DIR / 'vectorized'
PROCESSED_DIR = DATA_DIR / 'processed'
MODELS_DIR = DATA_DIR / 'models'
ANALYSIS_DIR = DATA_DIR / 'analysis'

for d in [PROCESSED_DIR, MODELS_DIR, ANALYSIS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

def load_data():
    """Charger données vectorisées"""
    df = pd.read_csv(VECTORIZED_DIR / 'OpenStack_event_occurrence_matrix.csv')
    return df

def get_event_columns(df):
    """recuperer colonnes evenements"""
    return [col for col in df.columns if col.startswith('E')]

def save_processed_data(df, name):
    path = PROCESSED_DIR / f'{name}.csv'
    df.to_csv(path, index=False)
    print(f" Sauvegardé: {path}")
    return path