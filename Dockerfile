FROM python:3.9-slim

# 1. On récupère uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# 2. ON COPIE TOUT CE QUI EST NÉCESSAIRE AU BUILD
# (C'est ici qu'on avait oublié le README !)
COPY pyproject.toml uv.lock README.md ./

# 3. Installation Système (Méthode Puriste)
RUN uv export --frozen --format=requirements-txt > requirements.txt
RUN uv pip install --system --no-cache -r requirements.txt

# 4. Création utilisateur
RUN useradd -m appuser

# 5. Copie du code source et des données
COPY src/ ./src/
COPY data/ ./data/

# 6. Sécurité
USER appuser

# 7. Lancement
CMD ["python", "src/main.py"]