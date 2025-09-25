# ML Pipeline Implementation TODOs

## Phase 1: Infrastructure Foundation
- [ ] Create GitHub repository under goldfish-inc
- [ ] Set up Pulumi project structure
- [ ] Configure ESC environment for secrets
- [ ] Define k3s cluster configuration for 3 VPS
- [ ] Create networking with Cloudflare Tunnels

## Phase 2: Workstation Setup (RTX 4090)
- [ ] Create unattended USB installer with:
  - [ ] Ubuntu 24.04 LTS base
  - [ ] NVIDIA drivers (550.x)
  - [ ] CUDA 12.x toolkit
  - [ ] Docker with GPU support
  - [ ] Python 3.11 with ML libraries
  - [ ] NFS client for shared storage
  - [ ] Automatic k3s agent join
- [ ] Configure GPU optimization settings
- [ ] Set up remote access (SSH, VNC)
- [ ] Plan for AMD motherboard upgrade
- [ ] Prepare for second RTX 4090 addition

## Phase 3: Data Ingestion Pipeline
- [ ] Deploy MinIO on k3s cluster
- [ ] Set up Airbyte with:
  - [ ] File system connector for /data/ingest
  - [ ] MinIO destination
  - [ ] 10-minute polling interval
- [ ] Create watched folder with NFS/Dropbox
- [ ] Implement metadata logging to Postgres
- [ ] Set up GraphQL API for ingestion tracking

## Phase 4: Data Processing
- [ ] Deploy Airflow on k3s
- [ ] Create DAGs for:
  - [ ] PDF parsing with Unstructured.io
  - [ ] CSV cleaning with Pandas
  - [ ] Text extraction with Granite
- [ ] Implement Great Expectations validation
- [ ] Set up GPU-accelerated processing on workstation
- [ ] Configure data lineage tracking

## Phase 5: Annotation System
- [ ] Deploy Label Studio on k3s
- [ ] Configure ML backend on workstation
- [ ] Set up BERT pre-labeling with Unsloth
- [ ] Create annotation workflows for:
  - [ ] Trade entities (vessel_name, hs_code)
  - [ ] Risk classification
  - [ ] Data quality flags
- [ ] Implement export to Postgres/Hugging Face

## Phase 6: Storage & Database
- [ ] Deploy Postgres 17.x with:
  - [ ] pgvector extension
  - [ ] PostgresML extension
  - [ ] PostGIS for geospatial
- [ ] Create schemas for:
  - [ ] Trade transactions
  - [ ] Embeddings (768-dim vectors)
  - [ ] Metadata and lineage
- [ ] Set up GraphQL Yoga API
- [ ] Implement data partitioning strategy

## Phase 7: ML Training Infrastructure
- [ ] Configure PostgresML for in-database training
- [ ] Set up Unsloth on RTX 4090s for:
  - [ ] Granite fine-tuning
  - [ ] Custom trade models
- [ ] Implement MLflow for experiment tracking
- [ ] Create training pipelines for:
  - [ ] XGBoost risk models
  - [ ] LLM trade entity extraction
  - [ ] Embedding generation
- [ ] Set up model versioning

## Phase 8: Model Serving & API
- [ ] Deploy model serving infrastructure
- [ ] Create GraphQL endpoints for:
  - [ ] Trade queries
  - [ ] Risk predictions
  - [ ] Entity extraction
- [ ] Implement caching layer
- [ ] Set up A/B testing framework

## Phase 9: Monitoring & Observability
- [ ] Deploy Prometheus + Grafana
- [ ] Create dashboards for:
  - [ ] Ingestion metrics
  - [ ] Processing throughput
  - [ ] Model performance
  - [ ] GPU utilization
- [ ] Set up alerting for:
  - [ ] Pipeline failures
  - [ ] Data quality issues
  - [ ] Resource constraints

## Phase 10: Custom Script Migration
- [ ] Audit existing Python scripts
- [ ] Refactor for Airflow compatibility
- [ ] Enhance with Granite/Unstructured.io
- [ ] Create unit tests
- [ ] Document in MkDocs
- [ ] Phase out redundant scripts

## Phase 11: Production Readiness
- [ ] Implement backup strategy
- [ ] Set up disaster recovery
- [ ] Create runbooks
- [ ] Performance optimization
- [ ] Security hardening
- [ ] Load testing

## Phase 12: Future Enhancements
- [ ] Prepare for trade API integrations (Bloomberg, etc.)
- [ ] Design Kafka streaming pipeline
- [ ] Plan for multi-GPU scaling
- [ ] Implement federated learning
- [ ] Add real-time inference

## Technical Debt & Maintenance
- [ ] Remove any remaining shell scripts
- [ ] Migrate all secrets to ESC
- [ ] Update to latest Pulumi providers
- [ ] Optimize Docker images
- [ ] Clean up unused resources

## Documentation
- [ ] API documentation with OpenAPI
- [ ] User guides for SME annotation
- [ ] Deployment procedures
- [ ] Troubleshooting guides
- [ ] Architecture decisions record (ADR)

---
*Track progress in GitHub Projects*