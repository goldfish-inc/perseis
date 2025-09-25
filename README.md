# Perseis - ML/AI Data Pipeline

Production-ready ML/AI pipeline for data ingestion, cleaning, and training workflow (CSVs, PDFs, text) using Pulumi IaC, k3s, Docling-Granite, and dual RTX 4090 workstation.

Named after **Perseis**, the Titaness of destruction - fitting for a platform that processes and transforms raw data through ML/AI workflows.

## Architecture

- **3 VPS Servers**: k3s cluster managed by Pulumi (tethys, styx, meliae)
- **1 Workstation**: Dual RTX 4090s (calypso - upgrading from Intel to AMD, 1x to 2x GPU)
- **Infrastructure**: 100% Pulumi IaC with ESC for secrets
- **Data Pipeline**: Airbyte → MinIO → Airflow → Postgres/Label Studio
- **ML Training**: PostgresML + Unsloth + Docling-Granite on RTX 4090s

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
perseis/
├── infrastructure/       # Pulumi IaC for k3s cluster
├── workstation/         # Unattended USB installer for RTX 4090 workstation
├── scripts/             # ML/AI processing scripts (from ebisu)
│   ├── import/          # Vessel data import scripts
│   ├── processing/      # Docling-Granite PDF processing
│   └── validation/      # Data validation and quality
├── airflow/            # DAGs for orchestration
│   └── dags/           # Maritime intelligence workflows
├── models/             # ML models and training scripts
└── docs/               # Documentation
```

## Components

### Infrastructure (k3s cluster on 3 VPS)
- MinIO (object storage for vessel data)
- Postgres 17.x with pgvector + PostgresML
- Airflow (orchestration)
- Label Studio (annotation)
- Airbyte (data ingestion)
- Grafana/Prometheus (monitoring)

### Workstation (Dual RTX 4090)
- **Docling-Granite**: Enterprise-grade PDF processing for vessel registries
- **GPU-accelerated parsing**: Tesseract, Unstructured.io, Granite models
- **ML training**: Unsloth fine-tuning, PostgresML in-database training
- **Model serving**: Real-time inference on RTX 4090s
- **CUDA 12.x optimized**: Full GPU acceleration pipeline

### Pipeline Stages
1. **Ingestion**: Manual upload → Airbyte → MinIO (PDFs, CSVs, text files)
2. **Cleaning**: **Docling-Granite** extraction + Great Expectations validation
3. **Annotation**: Label Studio with ML pre-labeling
4. **Storage**: Postgres with pgvector embeddings + PostgresML models
5. **Training**: In-database ML + Unsloth fine-tuning on RTX 4090s
6. **Serving**: GraphQL API + predictions

## ML/AI Features

### Docling-Granite Integration
- **Enterprise PDF Processing**: High-accuracy extraction from complex documents
- **Table Extraction**: Structured data from PDF tables
- **Multi-format Output**: JSON/CSV for downstream processing
- **GPU Acceleration**: MLX optimization for RTX 4090s

### Data Processing (from Ebisu Scripts)
- **Validation Pipeline**: Data quality checks and validation
- **Entity Extraction**: Automated field extraction from documents
- **Data Transformation**: CSV cleaning and standardization
- **Quality Scoring**: Data confidence assessment

### Training Infrastructure
- **PostgresML**: In-database model training and inference
- **Unsloth**: Fine-tuning LLMs on RTX 4090s
- **Vector Storage**: pgvector for embeddings and similarity search
- **Model Versioning**: MLflow experiment tracking

## Secrets Management

All secrets managed through Pulumi ESC:
- Postgres credentials
- MinIO keys
- Hugging Face tokens
- Maritime data source API keys

## Roadmap

- [x] Initial infrastructure setup
- [x] Docling-Granite integration
- [x] Ebisu script migration
- [x] Airflow DAG creation
- [ ] USB installer for workstation
- [ ] PostgresML model deployment
- [ ] ML training pipeline
- [ ] Production deployment
- [ ] AMD motherboard upgrade
- [ ] Second RTX 4090 integration

---
*Built with Pulumi IaC, Docling-Granite, and PostgresML for production-ready ML/AI data processing*