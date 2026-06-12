# Plan de test - Projet Final Airflow taux de change multi-devises

## Objectif

Ce plan de test couvre uniquement les elements essentiels demandes dans le sujet :
- extraction depuis l'API ;
- stockage brut ;
- transformation et chargement ;
- chemin nominal et chemin d'echec ;
- controle qualite sur les dimensions minimales ;
- tracabilite des rejets ;
- idempotence.

## Prerequis

- Docker Desktop demarre
- services `airflow` et `postgres` lances
- Airflow accessible sur `http://localhost:8080`
- base PostgreSQL initialisee avec `sql/init_db.sql`
- DAG `fx_rates_pipeline` visible dans Airflow

## Tables a verifier

- `fx_raw_ingestion`
- `fx_rates`
- `fx_rejections`
- `fx_run_logs`

## Commandes utiles

Depuis `airflow/Projet Final airflow` :

Lancer les services :

```powershell
docker compose up -d
```

Verifier les tables :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "\dt"
```

Relancer seulement Airflow apres modification du `.env` :

```powershell
docker compose build --no-cache airflow
docker compose up -d --force-recreate airflow
```

## Cas 1 - Execution nominale

### But

Prouver que les lignes valides :
- sont extraites ;
- sont stockees en brut ;
- sont transformees ;
- sont chargees dans `fx_rates` ;
- sont tracees dans `fx_run_logs`.

### Configuration

```env
FRANKFURTER_TARGET_CURRENCIES=USD,GBP,JPY,CHF,CAD
FX_FRESHNESS_THRESHOLD_DAYS=2
FX_FORCE_INVALID_RATE_FOR=
FX_FORCE_INVALID_CODE_FOR=
```

### Resultat attendu

- chemin nominal Airflow en succes ;
- insertion dans `fx_raw_ingestion` ;
- insertion dans `fx_rates` ;
- log de succes dans `fx_run_logs` ;
- pas de rejet pour ce run.

### Requetes PostgreSQL

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT id, run_id, base_currency, symbols_requested, api_date, ingested_at FROM fx_raw_ingestion ORDER BY ingested_at DESC LIMIT 10;"
```

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT rate_date, base_currency, quote_currency, currency_pair, exchange_rate FROM fx_rates ORDER BY rate_date DESC, currency_pair LIMIT 20;"
```

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT run_id, status, rows_received, rows_valid, rows_rejected, rows_inserted, created_at FROM fx_run_logs ORDER BY created_at DESC LIMIT 10;"
```

## Cas 2 - Qualite : completude

### But

Prouver qu'une devise attendue mais absente de la reponse est rejetee et tracee.

### Configuration

```env
FRANKFURTER_TARGET_CURRENCIES=USD,GBP,JPY,CHF,CAD,XXX
FX_FRESHNESS_THRESHOLD_DAYS=2
FX_FORCE_INVALID_RATE_FOR=
FX_FORCE_INVALID_CODE_FOR=
```

### Resultat attendu

- branchement vers le chemin de rejet ;
- insertion dans `fx_rejections` ;
- motif : `Devise cible manquante dans la reponse API` ;
- log d'execution dans `fx_run_logs`.

### Requetes PostgreSQL

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT run_id, currency_pair, rejection_reason, rejected_at FROM fx_rejections ORDER BY rejected_at DESC LIMIT 10;"
```

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT run_id, status, rows_received, rows_valid, rows_rejected, rows_inserted, message, created_at FROM fx_run_logs ORDER BY created_at DESC LIMIT 10;"
```

## Cas 3 - Qualite : coherence

### But

Prouver qu'un taux invalide est rejete et trace.

### Configuration

```env
FRANKFURTER_TARGET_CURRENCIES=USD,GBP,JPY,CHF,CAD
FX_FRESHNESS_THRESHOLD_DAYS=2
FX_FORCE_INVALID_RATE_FOR=USD
FX_FORCE_INVALID_CODE_FOR=
```

### Resultat attendu

- insertion dans `fx_rejections` ;
- motif : `Taux negatif ou nul` ;
- log d'execution dans `fx_run_logs`.

## Cas 4 - Qualite : structure

### But

Prouver qu'un code devise invalide est rejete et trace.

### Configuration

```env
FRANKFURTER_TARGET_CURRENCIES=USD,GBP,JPY,CHF,CAD
FX_FRESHNESS_THRESHOLD_DAYS=2
FX_FORCE_INVALID_RATE_FOR=
FX_FORCE_INVALID_CODE_FOR=USD
```

### Resultat attendu

- insertion dans `fx_rejections` ;
- motif : `Code devise invalide` ;
- log d'execution dans `fx_run_logs`.

## Cas 5 - Qualite : fraicheur

### But

Prouver qu'une donnee trop ancienne est rejetee et tracee.

### Configuration

```env
FRANKFURTER_TARGET_CURRENCIES=USD,GBP,JPY,CHF,CAD
FX_FRESHNESS_THRESHOLD_DAYS=-1
FX_FORCE_INVALID_RATE_FOR=
FX_FORCE_INVALID_CODE_FOR=
```

### Resultat attendu

- insertion dans `fx_rejections` ;
- motif : `Donnee trop ancienne selon le seuil de fraicheur` ;
- log d'execution dans `fx_run_logs`.

## Cas 6 - Idempotence / unicite

### But

Prouver qu'une relance avec la meme configuration ne cree pas de doublons metier.

### Configuration

Revenir a la configuration nominale :

```env
FRANKFURTER_TARGET_CURRENCIES=USD,GBP,JPY,CHF,CAD
FX_FRESHNESS_THRESHOLD_DAYS=2
FX_FORCE_INVALID_RATE_FOR=
FX_FORCE_INVALID_CODE_FOR=
```

### Action

1. lancer une execution nominale
2. relancer une seconde execution avec la meme configuration

### Resultat attendu

- aucune ligne en doublon logique dans `fx_rates`
- le pipeline reste relancable sans incoherence

### Requetes PostgreSQL

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT COUNT(*) AS total_rows FROM fx_rates;"
```

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT rate_date, base_currency, quote_currency, COUNT(*) FROM fx_rates GROUP BY rate_date, base_currency, quote_currency HAVING COUNT(*) > 1;"
```

## Requete de synthese qualite

Cette requete permet de resumer les motifs de rejet obtenus pendant les tests :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT rejection_reason, COUNT(*) FROM fx_rejections GROUP BY rejection_reason ORDER BY COUNT(*) DESC;"
```

## Ordre recommande

1. verifier les tables avec `\dt`
2. executer le cas nominal
3. executer le test de completude
4. executer le test de coherence
5. executer le test de structure
6. executer le test de fraicheur
7. revenir a la configuration nominale
8. executer le test d'idempotence

## Preuves a conserver

- capture Airflow du cas nominal
- capture Airflow du chemin de rejet
- `SELECT` sur `fx_raw_ingestion`
- `SELECT` sur `fx_rates`
- `SELECT` sur `fx_rejections`
- `SELECT` sur `fx_run_logs`
- requete anti-doublon vide
