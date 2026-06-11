# Projet final - cadrage initial

## Sujet

Construire une plateforme Airflow complete pour le suivi multi-devises a partir de l'API Frankfurter.

Le pipeline devra :
- extraire des taux de change pour au moins 5 paires de devises ;
- stocker la reponse brute ;
- transformer les donnees ;
- controler leur qualite ;
- charger les lignes valides ;
- rejeter et tracer les lignes invalides ;
- detecter des variations importantes de taux ;
- ecrire des logs d'execution ;
- produire des tables ou vues exploitables dans Metabase.

## Ce qu'on reutilise du projet precedent

On peut reprendre les grands principes de `pipeline-complet-api` :
- DAG lisible ;
- modules Python separes ;
- separation extraction / transformation / qualite / chargement ;
- retries, retry delay, timeout ;
- idempotence ;
- journalisation en base.

## Ce qui change

Par rapport au pipeline Open-Meteo :
- source API : Frankfurter au lieu de Open-Meteo ;
- entite metier : paires de devises au lieu de villes ;
- stockage brut demande en base ;
- controles qualite plus larges ;
- chemin nominal + chemin d'echec obligatoires ;
- table d'alertes obligatoire ;
- Connection ID Airflow pour PostgreSQL ;
- `init_db.sql` obligatoire ;
- resultats visibles dans Metabase.

## Hypothese de modelisation

### Table brute

`fx_raw_ingestion`

Role :
- stocker chaque reponse brute de l'API ;
- tracer l'horodatage d'ingestion ;
- conserver la base et les devises demandees ;
- garder le payload JSON brut.

Colonnes candidates :
- `id`
- `run_id`
- `base_currency`
- `symbols_requested`
- `api_date`
- `payload_json`
- `ingested_at`

### Table structuree principale

`fx_rates`

Role :
- stocker une ligne par paire de devises et par date.

Colonnes candidates :
- `id`
- `run_id`
- `rate_date`
- `base_currency`
- `quote_currency`
- `currency_pair`
- `exchange_rate`
- `ingested_at`

Contrainte d'unicite probable :
- `rate_date`
- `base_currency`
- `quote_currency`

### Table de rejets

`fx_rejections`

Role :
- conserver les lignes invalides apres controle qualite ;
- expliquer pourquoi elles n'ont pas ete chargees.

Colonnes candidates :
- `id`
- `run_id`
- `rate_date`
- `base_currency`
- `quote_currency`
- `currency_pair`
- `raw_rate`
- `rejection_reason`
- `rejected_at`

### Table d'alertes

`fx_alerts`

Role :
- tracer les variations de taux superieures a un seuil configurable.

Colonnes candidates :
- `id`
- `run_id`
- `rate_date`
- `currency_pair`
- `previous_rate`
- `current_rate`
- `rate_delta`
- `threshold_used`
- `alert_created_at`

### Table de logs d'execution

`fx_run_logs`

Role :
- tracer chaque execution du pipeline.

Colonnes candidates :
- `id`
- `run_id`
- `pipeline_name`
- `status`
- `rows_received`
- `rows_valid`
- `rows_rejected`
- `rows_inserted`
- `alert_count`
- `message`
- `created_at`

## Controles qualite a couvrir

Minimum demande par le sujet :

### Completude

Verifier :
- que toutes les devises demandees sont presentes ;
- que chaque taux est renseigne ;
- qu'on atteint bien le minimum de paires attendues.

### Coherence

Verifier :
- que le taux est strictement positif ;
- que les codes devises sont bien formes ;
- que la paire `base/quote` est coherente.

### Fraicheur

Verifier :
- que la date retournee par l'API n'est pas trop ancienne ;
- seuil de fraicheur configurable.

### Unicite

Verifier :
- absence de doublon logique sur `rate_date + base_currency + quote_currency`.

### Structure

Verifier :
- que la reponse API contient bien les cles attendues ;
- que le format des objets est conforme.

## Regle de branchement conditionnel

Si toutes les lignes sont valides :
- charger `fx_rates`
- detecter les alertes
- loguer le run en succes

Si certaines lignes sont invalides :
- envoyer les invalides dans `fx_rejections`
- charger les valides
- detecter les alertes sur les valides
- loguer le run avec detail des rejets

Si l'extraction ou la structure globale est invalide :
- ne pas charger `fx_rates`
- loguer le run en echec

## Proposition de DAG

Taches candidates :

1. `extract_fx_rates`
2. `store_raw_response`
3. `transform_fx_rates`
4. `quality_check_fx_rates`
5. `branch_on_quality`
6. `load_valid_fx_rates`
7. `load_rejected_fx_rates`
8. `detect_fx_rate_alerts`
9. `log_fx_run`

## Parametres a externaliser

Via Variables Airflow ou configuration equivalente :
- devise de base
- liste des devises cibles
- seuil d'alerte
- seuil de fraicheur
- schedule
- connection id PostgreSQL

Connexion retenue :
- `postgres_fx`

Regle d'implementation :
- utiliser `PostgresHook(postgres_conn_id="postgres_fx")` ou equivalent ;
- ne pas utiliser une connexion PostgreSQL codee directement dans le DAG avec `psycopg2.connect(...)`.

## KPI / vues Metabase

Minimum 2 exploitations utiles :

1. Evolution journaliere des taux par paire
2. Nombre d'alertes par paire ou par jour

Autres idees :
- top variations absolues
- nombre de rejets par execution
- fraicheur moyenne des donnees

## Livrables finaux a produire

- DAG Python complet
- `init_db.sql`
- captures Airflow
- captures PostgreSQL
- capture table de logs
- capture table d'alertes
- captures KPI Metabase
- note courte sur robustesse et qualite

## Ordre de construction recommande

1. creer l'arborescence projet
2. ecrire `init_db.sql`
3. preparer `docker-compose.yml` et `.env`
4. coder le DAG minimal extraction -> brut -> transformation -> chargement
5. ajouter qualite
6. ajouter branchement conditionnel
7. ajouter table de rejets
8. ajouter detection d'alertes
9. ajouter logs d'execution
10. preparer README et preuves
