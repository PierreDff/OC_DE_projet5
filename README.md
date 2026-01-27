$readmeContent = @"
# Projet de Migration Healthcare

## Description
Pipeline ETL pour migrer des données de santé depuis un CSV vers MongoDB.

## Installation
1. Installer uv
2. Lancer 'uv sync'

## Structure
- src/ : Code source
- tests/ : Tests unitaires et d'intégration
"@

Set-Content -Path "README.md" -Value $readmeContent