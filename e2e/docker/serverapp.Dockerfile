FROM flwr/serverapp:1.9.0

WORKDIR /app

COPY server.py ./
ENTRYPOINT [ "flower-server-app", "server:app" ]
