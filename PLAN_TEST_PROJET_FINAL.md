# Plan de test - Projet Final Airflow taux de change multi-devises

## Objectif

Ce document sert a :
- verifier le bon fonctionnement du pipeline ;
- produire les captures demandees dans le sujet ;
- structurer les tests avant le rendu final.

## Rappel des livrables a prouver

Le sujet demande au minimum :
- le DAG Python complet et fonctionnel ;
- le fichier `init_db.sql` ;
- une capture de l'UI Airflow avec execution reussie ;
- des captures des tables PostgreSQL apres execution ;
- une capture de la table de logs ;
- une capture de la table d'alertes avec justification du seuil ;
- des captures des KPIs sur Metabase ;
- une note expliquant la robustesse et les controles qualite.

## Prerequis

- Docker Desktop demarre
- conteneurs `airflow` et `postgres` lances
- Airflow accessible sur `http://localhost:8080`
- utilisateur Airflow cree
- PostgreSQL initialise avec `sql/init_db.sql`
- DAG `fx_rates_pipeline` visible dans Airflow

## Commandes de base

Depuis `airflow/Projet Final airflow` :

Lancer les services :

```powershell
docker compose up -d
```

Verifier l'etat des conteneurs :

```powershell
docker compose ps
```

Rejouer le SQL si la base existe deja :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -f /docker-entrypoint-initdb.d/init_db.sql
```

Verifier les tables :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "\dt"
```

## Tables a verifier

Les tables attendues sont :
- `fx_raw_ingestion`
- `fx_rates`
- `fx_rejections`
- `fx_alerts`
- `fx_run_logs`

## Cas 1 - Execution nominale

### But

Verifier que le pipeline :
- extrait les taux ;
- stocke la reponse brute ;
- transforme les lignes ;
- charge `fx_rates` ;
- ecrit un log d'execution ;
- execute le chemin nominal.

### Conditions

- API Frankfurter disponible
- devises configurees valides
- seuil de fraicheur non bloquant

### Action

Depuis l'UI Airflow :
1. ouvrir le DAG `fx_rates_pipeline`
2. lancer une execution manuelle

### Visuel Airflow attendu

- `extract_fx_rates` en succes
- `store_raw_response` en succes
- `transform_fx_rates` en succes
- `quality_check_fx_rates` en succes
- `branch_on_quality` en succes
- `load_fx_rates` en succes
- `detect_fx_alerts` en succes
- `log_fx_success_run` en succes

Chemin rejets attendu :
- `load_fx_rejections` en skipped si aucune ligne invalide
- `fail_on_quality_rejections` en skipped
- `log_fx_rejection_run` en skipped

### Requetes PostgreSQL a lancer

Verifier la table brute :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT id, run_id, base_currency, symbols_requested, api_date, ingested_at FROM fx_raw_ingestion ORDER BY ingested_at DESC LIMIT 10;"
```

Verifier la table structuree :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT rate_date, base_currency, quote_currency, currency_pair, exchange_rate FROM fx_rates ORDER BY rate_date DESC, currency_pair LIMIT 20;"
```

Verifier la table de logs :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT run_id, status, rows_received, rows_valid, rows_rejected, rows_inserted, alert_count, created_at FROM fx_run_logs ORDER BY created_at DESC LIMIT 10;"
```

### Captures a conserver

- graphe Airflow execution nominale
- `SELECT` sur `fx_raw_ingestion`
- `SELECT` sur `fx_rates`
- `SELECT` sur `fx_run_logs`

## Cas 2 - Lignes rejetees / chemin d'echec

### But

Verifier que des lignes invalides :
- sont envoyees dans `fx_rejections`
- ne sont pas chargees dans `fx_rates`
- provoquent un chemin d'echec trace

### Simulation du cas

Pour simuler ce cas, plusieurs approches sont possibles.

#### Option 1 - Fraicheur trop stricte

Modifier temporairement dans `.env` :

```env
FX_FRESHNESS_THRESHOLD_DAYS=0
```

Si la date retournee par l'API n'est pas exactement consideree comme fraiche selon l'implementation, des lignes peuvent etre rejetees.

#### Option 2 - Devise cible absente

Mettre dans `.env` une devise cible que Frankfurter ne retournera pas correctement dans ce contexte, par exemple une devise invalide ou non attendue selon votre logique de validation.

Exemple :

```env
FRANKFURTER_TARGET_CURRENCIES=USD,GBP,JPY,CHF,CAD,XXX
```

Cela permet de provoquer un rejet pour devise manquante ou structure invalide.

#### Option 3 - Simulation explicite a ajouter si besoin

Si le cas est difficile a provoquer naturellement, vous pouvez ajouter une variable de simulation dediee au pipeline, par exemple :

```env
FX_FORCE_REJECTION_CURRENCY=USD
```

Puis dans le code qualite, forcer le rejet de cette devise pour le test.

### Action

1. configurer une situation provoquant au moins un rejet
2. redemarrer les services si le `.env` a change
2. lancer le DAG

Commande si changement de `.env` :

```powershell
docker compose down
docker compose up -d
```

### Visuel Airflow attendu

- `quality_check_fx_rates` en succes
- `branch_on_quality` en succes
- `load_fx_rejections` en succes
- `log_fx_rejection_run` en succes
- `fail_on_quality_rejections` en failed

Chemin nominal attendu :
- `load_fx_rates` skipped ou partiel selon le design retenu
- `log_fx_success_run` non execute si run en echec

### Requetes PostgreSQL a lancer

Verifier les rejets :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT run_id, rate_date, currency_pair, raw_rate, rejection_reason, rejected_at FROM fx_rejections ORDER BY rejected_at DESC LIMIT 20;"
```

Verifier les logs :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT run_id, status, rows_received, rows_valid, rows_rejected, rows_inserted, message, created_at FROM fx_run_logs ORDER BY created_at DESC LIMIT 20;"
```

### Captures a conserver

- graphe Airflow du chemin d'echec
- `SELECT` sur `fx_rejections`
- `SELECT` sur `fx_run_logs`

### Retour a l'etat nominal

Remettre la configuration normale dans `.env`, puis :

```powershell
docker compose down
docker compose up -d
```

## Cas 3 - Idempotence / relance sans doublon

### But

Verifier qu'une relance du DAG ne cree pas de doublons dans `fx_rates`.

### Action

1. lancer une execution nominale
2. noter le nombre de lignes dans `fx_rates`
3. relancer le DAG avec les memes parametres
4. comparer les resultats

### Simulation du cas

Ce cas ne demande pas de truquage particulier.

Il faut simplement :
- garder exactement la meme configuration ;
- relancer une seconde fois le DAG ;
- verifier que `ON CONFLICT` evite les doublons logiques.

### Requetes PostgreSQL a lancer

Compter les lignes :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT COUNT(*) AS total_rows FROM fx_rates;"
```

Verifier l'absence de doublons logiques :

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT rate_date, base_currency, quote_currency, COUNT(*) FROM fx_rates GROUP BY rate_date, base_currency, quote_currency HAVING COUNT(*) > 1;"
```

### Resultat attendu

- le comptage reste stable si les memes donnees sont rechargees
- la requete anti-doublons ne retourne aucune ligne

### Captures a conserver

- premier comptage
- second comptage
- requete anti-doublons vide

## Cas 4 - Alertes sur variation

### But

Verifier que des ecarts importants entre deux executions successives produisent des alertes.

### Simulation du cas

Ce cas peut etre difficile a produire naturellement si les variations de taux sont faibles.

Approches possibles :

#### Option 1 - Baisser temporairement le seuil d'alerte

Modifier `.env` :

```env
FX_ALERT_THRESHOLD=0.0001
```

Cela rend la detection d'alertes beaucoup plus sensible.

Ensuite :

```powershell
docker compose down
docker compose up -d
```

Puis relancer le DAG.

#### Option 2 - Rejouer a des dates differentes

Si le pipeline charge des donnees avec des dates successives ou des ecarts de taux reels, relancer le DAG a deux moments differents peut suffire.

#### Option 3 - Simulation explicite a ajouter si besoin

Si le cas reste trop difficile a obtenir, vous pouvez ajouter une variable de simulation du type :

```env
FX_FORCE_ALERT_MULTIPLIER=1.2
```

Puis modifier temporairement le taux d'une devise pendant le calcul d'alertes pour provoquer un ecart artificiel.

### Requete PostgreSQL a lancer

```powershell
docker compose exec postgres psql -U fx_user -d fx_rates -c "SELECT run_id, rate_date, currency_pair, previous_rate, current_rate, rate_delta, threshold_used, alert_created_at FROM fx_alerts ORDER BY alert_created_at DESC LIMIT 20;"
```

### Captures a conserver

- `SELECT` sur `fx_alerts`
- justification du seuil retenu dans le README ou la note

### Retour a l'etat nominal

Revenir au seuil normal dans `.env`, par exemple :

```env
FX_ALERT_THRESHOLD=0.05
```

Puis relancer :

```powershell
docker compose down
docker compose up -d
```

## Cas 5 - Logs Airflow

### But

Prouver que le pipeline produit des logs exploitables.

### Requetes utiles

Logs du conteneur :

```powershell
docker compose logs airflow
```

Ou lecture via interface Airflow sur chaque tache.

### Elements a retrouver

- extraction des taux
- stockage brut
- transformation
- controle qualite
- chargement
- branchement
- alertes
- journalisation du run

### Captures a conserver

- une capture du dossier de logs ou de l'onglet `Logs`
- plusieurs captures montrant des messages applicatifs utiles

## Cas 6 - KPIs Metabase

### But

Prouver que les donnees chargees sont exploitables.

### KPI minimum recommandes

1. evolution des taux par paire
2. nombre d'alertes par jour ou par paire

### Captures a conserver

- un KPI sur `fx_rates`
- un KPI sur `fx_alerts`

## Ordre recommande des tests

1. verifier `\dt`
2. tester le cas nominal
3. tester l'idempotence
4. tester le chemin d'echec
5. tester les alertes
6. recuperer les logs
7. recuperer les captures Metabase

## Resultat final attendu

A la fin des tests, on doit pouvoir fournir :
- une execution nominale prouvee
- un chemin d'echec prouve
- une relance sans doublon prouvee
- une table d'alertes exploitable
- une table de logs exploitable
- des KPI Metabase visibles
