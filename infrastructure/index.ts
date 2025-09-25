import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";

// =============================================================================
// ML PIPELINE INFRASTRUCTURE - PULUMI IAC WITH ESC
// =============================================================================

const config = new pulumi.Config();
const namespace = "ml-pipeline";

// Kubernetes provider for existing k3s cluster
const k8sProvider = new k8s.Provider("k8s", {
    kubeconfig: config.require("kubeconfig")
});

// =============================================================================
// NAMESPACE
// =============================================================================

const mlNamespace = new k8s.core.v1.Namespace("ml-pipeline", {
    metadata: {
        name: namespace,
        labels: {
            "oceanid.pipeline/component": "ml-infrastructure",
            "pod-security.kubernetes.io/enforce": "baseline"
        }
    }
}, { provider: k8sProvider });

// =============================================================================
// SECRETS FROM ESC
// =============================================================================

const mlSecrets = new k8s.core.v1.Secret("ml-secrets", {
    metadata: {
        name: "ml-secrets",
        namespace: namespace
    },
    stringData: {
        "postgres-password": config.requireSecret("postgresPassword"),
        "minio-access-key": config.requireSecret("minioAccessKey"),
        "minio-secret-key": config.requireSecret("minioSecretKey"),
        "huggingface-token": config.requireSecret("huggingfaceToken"),
        "airflow-fernet-key": config.requireSecret("airflowFernetKey")
    }
}, { provider: k8sProvider });

// =============================================================================
// MINIO OBJECT STORAGE
// =============================================================================

export const minio = new k8s.helm.v3.Release("minio", {
    name: "minio",
    namespace: namespace,
    chart: "minio",
    version: "5.2.0",
    repositoryOpts: {
        repo: "https://charts.min.io"
    },
    values: {
        auth: {
            rootUser: "admin",
            rootPassword: config.requireSecret("minioAccessKey")
        },
        persistence: {
            enabled: true,
            size: "500Gi",
            storageClass: "local-path"
        },
        resources: {
            requests: {
                memory: "2Gi",
                cpu: "1000m"
            },
            limits: {
                memory: "4Gi",
                cpu: "2000m"
            }
        },
        defaultBuckets: "raw-trade-data,processed-data,models"
    }
}, { provider: k8sProvider, dependsOn: [mlNamespace] });

// =============================================================================
// POSTGRES WITH PGVECTOR & POSTGRESML
// =============================================================================

export const postgres = new k8s.helm.v3.Release("postgres", {
    name: "postgres",
    namespace: namespace,
    chart: "postgresql",
    version: "15.5.0",
    repositoryOpts: {
        repo: "https://charts.bitnami.com/bitnami"
    },
    values: {
        auth: {
            postgresPassword: config.requireSecret("postgresPassword"),
            database: "tradedb"
        },
        image: {
            tag: "17.0",
            repository: "postgresml/postgresml"
        },
        primary: {
            persistence: {
                enabled: true,
                size: "100Gi"
            },
            initdb: {
                scripts: {
                    "init.sql": `
                        CREATE EXTENSION IF NOT EXISTS pgvector;
                        CREATE EXTENSION IF NOT EXISTS postgresml;
                        CREATE EXTENSION IF NOT EXISTS postgis;

                        CREATE TABLE trade_transactions (
                            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            trade_id TEXT NOT NULL,
                            trade_date DATE,
                            vessel_name TEXT,
                            hs_code TEXT,
                            commodity JSONB,
                            risk_score FLOAT,
                            embedding VECTOR(768),
                            created_at TIMESTAMP DEFAULT NOW()
                        );

                        CREATE INDEX idx_trade_date ON trade_transactions(trade_date);
                        CREATE INDEX idx_embedding ON trade_transactions USING ivfflat (embedding vector_cosine_ops);
                    `
                }
            }
        },
        resources: {
            requests: {
                memory: "4Gi",
                cpu: "2000m"
            },
            limits: {
                memory: "8Gi",
                cpu: "4000m"
            }
        }
    }
}, { provider: k8sProvider, dependsOn: [mlNamespace, mlSecrets] });

// =============================================================================
// AIRBYTE DATA INGESTION
// =============================================================================

export const airbyte = new k8s.helm.v3.Release("airbyte", {
    name: "airbyte",
    namespace: namespace,
    chart: "airbyte",
    version: "0.50.0",
    repositoryOpts: {
        repo: "https://airbytehq.github.io/helm-charts"
    },
    values: {
        global: {
            database: {
                host: "postgres-postgresql",
                port: 5432,
                database: "airbyte",
                user: "postgres",
                existingSecret: "ml-secrets",
                existingSecretPasswordKey: "postgres-password"
            },
            logs: {
                minio: {
                    enabled: true,
                    endpoint: "http://minio:9000",
                    accessKeyId: { secretKeyRef: { name: "ml-secrets", key: "minio-access-key" } },
                    secretAccessKey: { secretKeyRef: { name: "ml-secrets", key: "minio-secret-key" } }
                }
            }
        },
        webapp: {
            resources: {
                requests: { memory: "1Gi", cpu: "500m" },
                limits: { memory: "2Gi", cpu: "1000m" }
            }
        }
    }
}, { provider: k8sProvider, dependsOn: [postgres, minio] });

// =============================================================================
// LABEL STUDIO ANNOTATION
// =============================================================================

export const labelStudio = new k8s.helm.v3.Release("label-studio", {
    name: "label-studio",
    namespace: namespace,
    chart: "label-studio",
    version: "1.13.0",
    repositoryOpts: {
        repo: "https://charts.heartex.com"
    },
    values: {
        global: {
            postgresql: {
                auth: {
                    existingSecret: "ml-secrets",
                    secretKeys: {
                        adminPasswordKey: "postgres-password"
                    }
                }
            }
        },
        app: {
            config: {
                LABEL_STUDIO_HOST: "https://label.boathou.se",
                DEFAULT_PROJECT_IMPORT_STORAGE_TYPE: "s3",
                MINIO_ENDPOINT: "http://minio:9000",
                MINIO_ACCESS_KEY: { secretKeyRef: { name: "ml-secrets", key: "minio-access-key" } },
                MINIO_SECRET_KEY: { secretKeyRef: { name: "ml-secrets", key: "minio-secret-key" } }
            }
        },
        resources: {
            requests: { memory: "1Gi", cpu: "500m" },
            limits: { memory: "2Gi", cpu: "1000m" }
        }
    }
}, { provider: k8sProvider, dependsOn: [postgres, minio] });

// =============================================================================
// AIRFLOW ORCHESTRATION
// =============================================================================

export const airflow = new k8s.helm.v3.Release("airflow", {
    name: "airflow",
    namespace: namespace,
    chart: "airflow",
    version: "1.13.0",
    repositoryOpts: {
        repo: "https://airflow.apache.org"
    },
    values: {
        executor: "KubernetesExecutor",
        data: {
            metadataSecretName: "ml-secrets"
        },
        webserver: {
            defaultUser: {
                enabled: true,
                role: "Admin",
                username: "admin",
                password: config.requireSecret("airflowPassword")
            }
        },
        config: {
            core: {
                fernet_key: config.requireSecret("airflowFernetKey"),
                load_examples: false
            },
            connections: {
                minio_default: `s3://minio-access-key:minio-secret-key@minio:9000`,
                postgres_default: `postgresql://postgres:postgres-password@postgres-postgresql:5432/tradedb`
            }
        },
        dags: {
            persistence: {
                enabled: true,
                size: "10Gi"
            },
            gitSync: {
                enabled: true,
                repo: "https://github.com/goldfish-inc/ml-pipeline-dags.git",
                branch: "main",
                wait: 60
            }
        },
        resources: {
            webserver: {
                requests: { memory: "1Gi", cpu: "500m" },
                limits: { memory: "2Gi", cpu: "1000m" }
            },
            scheduler: {
                requests: { memory: "1Gi", cpu: "500m" },
                limits: { memory: "2Gi", cpu: "1000m" }
            }
        }
    }
}, { provider: k8sProvider, dependsOn: [postgres, minio] });

// =============================================================================
// MLFLOW TRACKING
// =============================================================================

const mlflowDeployment = new k8s.apps.v1.Deployment("mlflow", {
    metadata: {
        name: "mlflow",
        namespace: namespace
    },
    spec: {
        replicas: 1,
        selector: {
            matchLabels: { app: "mlflow" }
        },
        template: {
            metadata: {
                labels: { app: "mlflow" }
            },
            spec: {
                containers: [{
                    name: "mlflow",
                    image: "ghcr.io/mlflow/mlflow:v2.15.0",
                    args: [
                        "server",
                        "--backend-store-uri", "postgresql://postgres:$(POSTGRES_PASSWORD)@postgres-postgresql:5432/mlflow",
                        "--default-artifact-root", "s3://models",
                        "--host", "0.0.0.0"
                    ],
                    env: [
                        { name: "POSTGRES_PASSWORD", valueFrom: { secretKeyRef: { name: "ml-secrets", key: "postgres-password" } } },
                        { name: "AWS_ACCESS_KEY_ID", valueFrom: { secretKeyRef: { name: "ml-secrets", key: "minio-access-key" } } },
                        { name: "AWS_SECRET_ACCESS_KEY", valueFrom: { secretKeyRef: { name: "ml-secrets", key: "minio-secret-key" } } },
                        { name: "MLFLOW_S3_ENDPOINT_URL", value: "http://minio:9000" }
                    ],
                    ports: [{ containerPort: 5000 }],
                    resources: {
                        requests: { memory: "512Mi", cpu: "250m" },
                        limits: { memory: "1Gi", cpu: "500m" }
                    }
                }]
            }
        }
    }
}, { provider: k8sProvider, dependsOn: [postgres, minio] });

const mlflowService = new k8s.core.v1.Service("mlflow", {
    metadata: {
        name: "mlflow",
        namespace: namespace
    },
    spec: {
        selector: { app: "mlflow" },
        ports: [{ port: 5000, targetPort: 5000 }]
    }
}, { provider: k8sProvider });

// =============================================================================
// GRAPHQL API
// =============================================================================

const graphqlDeployment = new k8s.apps.v1.Deployment("graphql-api", {
    metadata: {
        name: "graphql-api",
        namespace: namespace
    },
    spec: {
        replicas: 2,
        selector: {
            matchLabels: { app: "graphql-api" }
        },
        template: {
            metadata: {
                labels: { app: "graphql-api" }
            },
            spec: {
                containers: [{
                    name: "graphql",
                    image: "goldfish-inc/trade-graphql-api:latest",
                    env: [
                        { name: "DATABASE_URL", value: "postgresql://postgres:$(POSTGRES_PASSWORD)@postgres-postgresql:5432/tradedb" },
                        { name: "POSTGRES_PASSWORD", valueFrom: { secretKeyRef: { name: "ml-secrets", key: "postgres-password" } } }
                    ],
                    ports: [{ containerPort: 4000 }],
                    resources: {
                        requests: { memory: "256Mi", cpu: "100m" },
                        limits: { memory: "512Mi", cpu: "200m" }
                    }
                }]
            }
        }
    }
}, { provider: k8sProvider, dependsOn: [postgres] });

// =============================================================================
// MONITORING
// =============================================================================

export const monitoring = new k8s.helm.v3.Release("monitoring", {
    name: "kube-prometheus-stack",
    namespace: namespace,
    chart: "kube-prometheus-stack",
    version: "65.0.0",
    repositoryOpts: {
        repo: "https://prometheus-community.github.io/helm-charts"
    },
    values: {
        prometheus: {
            prometheusSpec: {
                storageSpec: {
                    volumeClaimTemplate: {
                        spec: {
                            accessModes: ["ReadWriteOnce"],
                            resources: { requests: { storage: "50Gi" } }
                        }
                    }
                }
            }
        },
        grafana: {
            adminPassword: config.requireSecret("grafanaPassword"),
            persistence: {
                enabled: true,
                size: "10Gi"
            }
        }
    }
}, { provider: k8sProvider, dependsOn: [mlNamespace] });

// =============================================================================
// OUTPUTS
// =============================================================================

export const status = {
    namespace: namespace,
    components: {
        storage: "MinIO (500Gi)",
        database: "Postgres 17 with pgvector/postgresml",
        ingestion: "Airbyte",
        annotation: "Label Studio",
        orchestration: "Airflow (KubernetesExecutor)",
        tracking: "MLflow",
        monitoring: "Prometheus + Grafana"
    },
    endpoints: {
        airbyte: "https://airbyte.boathou.se",
        labelStudio: "https://label.boathou.se",
        airflow: "https://airflow.boathou.se",
        mlflow: "https://mlflow.boathou.se",
        grafana: "https://grafana.boathou.se"
    }
};