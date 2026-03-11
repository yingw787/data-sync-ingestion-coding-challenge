FROM python:3.12-slim

ENV POETRY_VIRTUALENVS_CREATE=false

RUN pip install poetry==2.1.1

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN poetry install --only main --no-root

COPY src ./src
RUN poetry install --only main

CMD ["python", "-m", "ingestion"]
