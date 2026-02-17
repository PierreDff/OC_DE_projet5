FROM python:3.9-slim

# 1. Récupération d'UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# 2. Copie des fichiers de dépendances
COPY pyproject.toml uv.lock README.md ./

# 3. Installation des dépendances
# --frozen : assure que les versions installées correspondent exactement au fichier lock
# --no-cache : réduit la taille de l'image finale
RUN uv export --frozen --format=requirements-txt > requirements.txt
RUN uv pip install --system --no-cache -r requirements.txt

# 4. Création d'un utilisateur non-root pour la sécurité
RUN useradd -m appuser

# 5. Copie du code source et des données
COPY src/ ./src/
COPY data/ ./data/

# 6. Sécurité
USER appuser

# 7. Lancement
CMD ["python", "src/main.py"]