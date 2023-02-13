# Dockerfile

# Build layer
FROM python:3.11.0-slim-buster AS builder

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUNBUFFERED=1

# Copy the source
COPY . /app
WORKDIR /app

# Make executable
RUN chmod +x ./wait-for-it.sh

# Install poetry
RUN pip install --upgrade pip --root-user-action=ignore
RUN pip install poetry --root-user-action=ignore

# Install dependencies
RUN python -m venv --copies /app/venv
RUN . /app/venv/bin/activate && poetry install --only main --no-interaction --no-ansi

# Result layer
FROM python:3.11.0-slim-buster 

# Install curl for healthchecks
RUN apt-get update && apt-get install -y curl
# Copy venv from build layer as well as the source
COPY --from=builder /app/venv /app/venv/
ENV PATH /app/venv/bin:$PATH
COPY --from=builder /app /app
WORKDIR /app

ENTRYPOINT []