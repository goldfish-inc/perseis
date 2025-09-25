# ML/AI Trade Data Pipeline

Production-ready ML/AI pipeline for processing trade data (CSVs, PDFs, text) using Pulumi IaC, k3s, and dual RTX 4090 workstation.

## Architecture

- **3 VPS Servers**: k3s cluster managed by Pulumi
- **1 Workstation**: Dual RTX 4090s (upgrading from Intel to AMD, 1x to 2x GPU)
- **Infrastructure**: 100% Pulumi IaC with ESC for secrets
- **Data Pipeline**: Airbyte → MinIO → Airflow → Postgres/Label Studio
- **ML Training**: PostgresML + Unsloth on RTX 4090s

## Quick Start

```bash
# Deploy infrastructure
cd infrastructure
pulumi up --yes

# Configure workstation (USB installer)
cd workstation
./create-usb-installer.sh

# Deploy ML pipeline
cd pipeline
pulumi up --yes
```

## Project Structure

```
ml-pipeline/
├── infrastructure/       # Pulumi IaC for k3s cluster
├── workstation/         # Unattended USB installer for RTX 4090 workstation
├── pipeline/            # ML pipeline components
├── airflow/            # DAGs for orchestration
├── models/             # ML models and training scripts
└── docs/               # Documentation
```

## Components

### Infrastructure (k3s cluster on 3 VPS)
- MinIO (object storage)
- Postgres 17.x with pgvector
- Airflow (orchestration)
- Label Studio (annotation)
- Airbyte (data ingestion)
- Grafana/Prometheus (monitoring)

### Workstation (Dual RTX 4090)
- GPU-accelerated parsing (Tesseract/Unstructured.io)
- ML training (Unsloth, PostgresML)
- Model serving
- CUDA 12.x optimized

### Pipeline Stages
1. **Ingress**: Manual upload → Airbyte → MinIO
2. **Cleaning**: Granite/Unstructured.io + Great Expectations
3. **Annotation**: Label Studio with ML pre-labeling
4. **Storage**: Postgres with pgvector embeddings
5. **Training**: PostgresML + Unsloth fine-tuning
6. **Serving**: GraphQL API + predictions

## Secrets Management

All secrets managed through Pulumi ESC:
- Postgres credentials
- MinIO keys
- Hugging Face tokens
- API keys

## Roadmap

- [x] Initial infrastructure setup
- [ ] USB installer for workstation
- [ ] Data ingestion pipeline
- [ ] ML training pipeline
- [ ] Production deployment
- [ ] AMD motherboard upgrade
- [ ] Second RTX 4090 integration

---
*Built with Pulumi IaC and ESC for production-ready ML infrastructure*