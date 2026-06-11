# Plan d'execution - Projet Final Airflow taux de change multi-devises

## Objectif

Construire une plateforme Airflow complete autour de l'API Frankfurter pour :
- extraire des taux de change multi-devises ;
- stocker les donnees brutes ;
- transformer et charger les donnees valides ;
- rejeter et tracer les donnees invalides ;
- detecter les variations importantes ;
- exposer des KPI exploitables dans Metabase.

## Phase 1 - Infrastructure et cadrage

### 1. Finaliser la configuration

A definir :
- devise de base ;
- au moins 5 devises cibles ;
- seuil d'alerte ;
- seuil de fraicheur ;
- schedule du DAG.

### 2. Finaliser le modele de donnees

Tables retenues :
- `fx_raw_ingestion`
- `fx_rates`
- `fx_rejections`
- `fx_alerts`
- `fx_run_logs`

### 3. Finaliser l'environnement technique

A preparer :
- `.env`
- `docker-compose.yml`
- `sql/init_db.sql`
- connexion Airflow PostgreSQL `postgres_fx`

## Phase 2 - Structure applicative

### 4. Construire l'arborescence Python

Modules a creer dans `include/` :
- `config.py`
- `runtime.py`
- `storage.py`
- `extract.py`
- `transform.py`
- `quality.py`
- `load.py`
- `alerts.py`
- `run_logging.py`

### 5. Poser le DAG TaskFlow

Le DAG doit utiliser :
- `@dag`
- `@task`

Contraintes :
- taches lisibles ;
- noms explicites ;
- responsabilites separees.

## Phase 3 - Pipeline nominal

### 6. Developper l'extraction Frankfurter

Le pipeline doit :
- appeler Frankfurter ;
- recuperer au moins 5 paires ;
- stocker la reponse brute.

### 7. Stocker la reponse brute

Stockage attendu :
- table `fx_raw_ingestion`
- horodatage d'ingestion

### 8. Transformer les donnees

Objectif :
- une ligne par paire de devises ;
- une ligne par date.

### 9. Charger la table structuree

Destination :
- `fx_rates`

## Phase 4 - Qualite et chemin d'echec

### 10. Ajouter les controles qualite

Minimum obligatoire :
- completude ;
- coherence ;
- fraicheur ;
- unicite ;
- structure.

### 11. Implementer le branchement conditionnel

Chemin nominal :
- chargement des lignes valides

Chemin d'echec :
- rejet des lignes invalides ;
- trace des anomalies ;
- aucun chargement des lignes invalides.

### 12. Implementer la table de rejets

Destination :
- `fx_rejections`

## Phase 5 - Historisation, idempotence et alertes

### 13. Garantir l'idempotence

Le pipeline doit :
- etre relancable ;
- ne pas creer de doublons ;
- enrichir proprement l'historique.

### 14. Detecter les alertes de variation

Regle :
- comparer deux executions consecutives ;
- calculer l'ecart ;
- enregistrer dans `fx_alerts`.

## Phase 6 - Logs et robustesse

### 15. Ajouter la robustesse Airflow

A mettre en place :
- retries ;
- retry delay ;
- timeout ;
- logs applicatifs ;
- gestion propre des erreurs.

### 16. Ajouter la table de logs d'execution

Destination :
- `fx_run_logs`

Contenu minimal :
- statut ;
- lignes recues ;
- valides ;
- rejetees ;
- inserees.

## Phase 7 - Exploitation analytique

### 17. Creer les KPI SQL

Minimum 2 requetes ou vues :
- evolution des taux par paire ;
- alertes par jour ou par paire.

### 18. Verifier l'exploitation Metabase

A produire :
- captures des KPI ;
- preuve que les resultats sont visibles.

## Phase 8 - Validation et rendu

### 19. Tester les cas obligatoires

Cas a verifier :
- cas nominal ;
- cas avec lignes rejetees ;
- cas de relance sans doublon ;
- cas avec alertes.

### 20. Produire les livrables

Livrables attendus :
- DAG Python complet ;
- `init_db.sql` ;
- capture Airflow execution reussie ;
- captures PostgreSQL ;
- capture table de logs ;
- capture table d'alertes ;
- captures KPI Metabase ;
- note 1/2 page sur robustesse et qualite.

## Ordre recommande pour nous

1. finir `init_db.sql`
2. finir `.env` et `docker-compose.yml`
3. preparer la connexion Airflow `postgres_fx`
4. creer les modules Python
5. coder le pipeline nominal
6. ajouter les controles qualite
7. ajouter les rejets
8. ajouter les alertes
9. ajouter les logs d'execution
10. preparer les KPI
11. preparer les preuves et le README final

## Priorite immediate

La prochaine etape de dev recommandee est :

1. creer les modules Python dans `include/`
2. coder un premier DAG fonctionnel :
   - extraction
   - stockage brut
   - transformation
   - chargement
   - log d'execution
