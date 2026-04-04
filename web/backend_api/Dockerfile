FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY backend_api/requirements.txt ./backend_api/requirements.txt
RUN pip install --no-cache-dir -r backend_api/requirements.txt

# Copy BE source
COPY backend_api/ ./backend_api/

# Copy map files that main.py reads via ROOT_DIR/src/lib/
COPY src/lib/constituency-map.json ./src/lib/constituency-map.json
COPY src/lib/ls-constituency-map.json ./src/lib/ls-constituency-map.json

ENV PORT=8080

CMD ["sh", "-c", "uvicorn backend_api.main:app --host 0.0.0.0 --port ${PORT}"]
