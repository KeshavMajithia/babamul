# BOOM Filter Tester WebApp

This directory contains a standalone Web UI for building, testing, and managing BOOM alert filters. It provides a visual sandbox for constructing MongoDB aggregation pipelines against live BOOM data.

## Features

- **Schema Explorer:** An interactive tree view of the full ZTF and LSST alert schemas.
- **Pipeline Editor:** Write and validate MongoDB JSON aggregation pipelines.
- **Filter Testing:** Execute pipelines against live databases with JD and candidate constraints to preview actual alert payloads.
- **Saved Filters:** Browse and load previously deployed filters from the BOOM backend.

## Security

The application uses a backend proxy built with FastAPI. The BOOM main API uses JWT authentication which is issued using admin credentials. To prevent token leakage, all credentials and token management are handled by the backend proxy; the browser never sees the JWT or raw passwords.

## Local Development

You will need the `admin` password for the BOOM cluster.

1. Ensure dependencies are installed via `uv` or `pip`:
   ```bash
   pip install fastapi uvicorn httpx python-dotenv
   ```

2. Set your environment variables (or create a `.env` file in this directory):
   ```bash
   export BOOM_API_BASE_URL="https://boom-api.nrp-nautilus.io"
   export BOOM_USERNAME="admin"
   export BOOM_PASSWORD="your_admin_password"
   ```

3. Run the server:
   ```bash
   python -m uvicorn main:app --reload --port 8765
   ```

4. Visit `http://127.0.0.1:8765` in your browser.

## Deployment (Docker/Kubernetes)

A `Dockerfile` is provided for containerized deployment.

1. Build the image:
   ```bash
   docker build -t your-registry/boom-filter-webapp:latest .
   docker push your-registry/boom-filter-webapp:latest
   ```

2. In your Kubernetes cluster, deploy this container as a standard Deployment and expose it via an Ingress. Pass the `BOOM_PASSWORD` as a secret.

Example Kubernetes configuration snippet:
```yaml
env:
  - name: BOOM_API_BASE_URL
    value: "http://boom-api:8080"
  - name: BOOM_USERNAME
    value: "admin"
  - name: BOOM_PASSWORD
    valueFrom:
      secretKeyRef:
        name: boom-secrets
        key: BOOM_API__AUTH__ADMIN_PASSWORD
```
