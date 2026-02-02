FROM python:3.9-slim

# 1. Récupération d'UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# 2. Copie de ce qui est nécessaire au build
COPY pyproject.toml uv.lock README.md ./

# 3. Installation Système
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