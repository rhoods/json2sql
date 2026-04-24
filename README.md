# json2sql

Outil Rust pour importer des fichiers JSON/NDJSON volumineux dans PostgreSQL via inférence automatique de schéma et `COPY FROM STDIN`.

- **Pass 1** — scan complet du fichier pour inférer tables, colonnes et types
- **Pass 2** — insertion via COPY (100k–500k lignes/s selon la largeur du schéma)
- Normalisation des objets imbriqués et tableaux en tables filles avec clés étrangères
- Rapport d'anomalies de type (valeurs incohérentes → NULL + log)

## Installation

```bash
cargo build --release
# Binaire : ./target/release/json2sql
```

## Utilisation rapide

```bash
# Import basique
json2sql --input data.json --db-url postgres://user:pass@localhost/mydb

# Depuis stdin
cat data.jsonl | json2sql --db-url $DATABASE_URL --table my_root

# Afficher le DDL sans toucher la base
json2sql --input data.json --dry-run

# Valider le schéma inféré avant import
json2sql --input data.json --dry-run --schema-report

# Import avec surcharges manuelles et rapport d'anomalies
json2sql \
  --input data.jsonl \
  --db-url $DATABASE_URL \
  --schema-config overrides.toml \
  --drop-existing \
  --anomaly-output anomalies.json \
  --max-anomaly-rate 0.01
```

`DATABASE_URL` est lue automatiquement si `--db-url` est absent.

## Documentation

| Document | Contenu |
|---|---|
| [docs/index.md](docs/index.md) | Index complet — point d'entrée |
| [docs/functional/usage.md](docs/functional/usage.md) | Référence complète des flags CLI et config TOML |
| [docs/technical/architecture.md](docs/technical/architecture.md) | Pipeline et modules |
