FROM node:20-bookworm-slim@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0 AS frontend-build
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend ./
RUN npm run build

FROM python:3.12-slim-bookworm@sha256:e697c81897302a4c4333bade000d98038fbb21eb064f8ee1fd42d02ebcd9d337
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app/backend OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 QUANTOS_CONTEST_DIR=/contest
WORKDIR /app
COPY requirements-contest.lock ./
RUN pip install --no-cache-dir -r requirements-contest.lock
RUN useradd -u 10001 quantos
COPY backend ./backend
COPY --from=frontend-build /build/frontend/dist ./frontend/dist
COPY --from=frontend-build /build/frontend/node_modules/swagger-ui-dist/swagger-ui-bundle.js ./frontend/dist/vendor/swagger-ui-bundle.js
COPY --from=frontend-build /build/frontend/node_modules/swagger-ui-dist/swagger-ui.css ./frontend/dist/vendor/swagger-ui.css
USER quantos
CMD ["python","-m","uvicorn","marketlab.api:app","--host","0.0.0.0","--port","8000"]
