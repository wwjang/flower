FROM flwr/supernode:1.9.0

WORKDIR /app
COPY pyproject.toml ./
RUN python -m pip install -U --no-cache-dir .

COPY client.py ./
ENTRYPOINT [ "flower-client-app", "client:app" ]
