## HDFS & OpenStack Log Anomaly Detection

Pipeline ML pour detecter automatiquement les anomalies dans les logs de systemes distribues (HDFS et OpenStack).

1. Telecharger les datasets
Les logs bruts ne sont pas inclus dans le repo (fichiers trop gros). Telecharger depuis Loghub :
- HDFS: https://zenodo.org/records/8196385/files/HDFS_v1.zip?download=1
- Openstack: https://zenodo.org/records/8196385/files/OpenStack.tar.gz?download=1

2. Build les images Docker: docker-compose build  
 HDFS:   
  Parsing: docker-compose up parser-hdfs  
  Vectorisation: docker-compose up vectorize-hdfs  
  Training-ML: docker-compose up jupyter-hdfs

 Lancer: Ouvrir http://localhost:8888  
 Executer: EDA.ipynb, preprocessing.ipynb, training.ipynb

 Openstack:   
  Parsing: docker-compose up parser-openstack  
  Vectorisation: docker-compose up vectorize-openstack   
  Training-ML: docker-compose up jupyter-openstack  

 Lancer: Ouvrir http://localhost:8890  
 Executer: EDA.ipynb, preprocessing.ipynb, training.ipynb  

 Auteurs

 Projet academique - Mise en place d'un pipeline AiOPs
