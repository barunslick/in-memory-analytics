FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml ./
COPY uv.lock ./

RUN uv pip install --system -r pyproject.toml

COPY src ./src

# Default command (can be overridden by docker-compose exec)
CMD ["python", "src/duck_db.py"]