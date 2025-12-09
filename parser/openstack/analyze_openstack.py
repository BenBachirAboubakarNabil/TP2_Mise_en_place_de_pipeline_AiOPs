import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Config
DATA_FILE = '/data/hdfs/vectorized/HDFS_event_occurrence_matrix.csv'
OUTPUT_DIR = '/data/hdfs/analysis/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Charger données
df = pd.read_csv(DATA_FILE)
event_cols = [col for col in df.columns if col.startswith('E')]

# === STATISTIQUES ===
print("Distribution des labels:")
label_counts = df['Label'].value_counts()
print(label_counts)
print(f"\nTaux anomalie: {(df['Label']=='Anomaly').sum()/len(df)*100:.2f}%")


# === VISUALISATIONS ===
sns.set_style('whitegrid')
fig, axes = plt.subplots(2, 2, figsize=(15, 12))

# Plot 1: Distribution labels
label_counts.plot(kind='bar', ax=axes[0,0], color=['green', 'red'])
axes[0,0].set_title('Distribution Normal vs Anomaly', fontsize=14, fontweight='bold')
axes[0,0].set_ylabel('Nombre de BlockIds')
axes[0,0].set_xlabel('Label')


# Plot 3: Top 15 événements
event_totals = df[event_cols].sum().sort_values(ascending=False).head(15)
event_totals.plot(kind='bar', ax=axes[1,0], color='steelblue')
axes[1,0].set_title('Top 15 événements les plus fréquents', fontsize=14, fontweight='bold')
axes[1,0].set_ylabel('Occurrences totales')
axes[1,0].set_xlabel('EventId')

# Plot 4: Comparaison événements Normal vs Anomaly (top 10)
comparison = df.groupby('Label')[event_cols].sum().T
top_events = comparison.sum(axis=1).sort_values(ascending=False).head(10).index
comparison.loc[top_events].plot(kind='bar', ax=axes[1,1], color=['green', 'red'])
axes[1,1].set_title('Top 10 événements: Normal vs Anomaly', fontsize=14, fontweight='bold')
axes[1,1].set_ylabel('Occurrences')
axes[1,1].set_xlabel('EventId')
axes[1,1].legend(title='Label')

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, 'analysis.png'), dpi=300, bbox_inches='tight')
print(f"\n✓ Graphiques sauvegardés: {OUTPUT_DIR}analysis.png")

# Distribution détaillée
print("\n\n=== TOP 10 ÉVÉNEMENTS PAR LABEL ===")
for label in df['Label'].unique():
    print(f"\n{label}:")
    subset = df[df['Label'] == label]
    top = subset[event_cols].sum().sort_values(ascending=False).head(10)
    for event, count in top.items():
        pct = count / subset[event_cols].sum().sum() * 100
        print(f"  {event}: {int(count):>8,} ({pct:>5.2f}%)")