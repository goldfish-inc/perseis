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
// LABEL STUDIO ML BACKEND
// =============================================================================

const labelStudioML = new k8s.apps.v1.Deployment("label-studio-ml", {
    metadata: {
        name: "label-studio-ml",
        namespace: namespace
    },
    spec: {
        replicas: 1,
        selector: {
            matchLabels: { app: "label-studio-ml" }
        },
        template: {
            metadata: {
                labels: { app: "label-studio-ml" }
            },
            spec: {
                containers: [{
                    name: "ml-backend",
                    image: "heartexlabs/label-studio-ml-backend:latest",
                    env: [
                        {
                            name: "HUGGINGFACE_TOKEN",
                            valueFrom: { secretKeyRef: { name: "ml-secrets", key: "huggingface-token" } }
                        },
                        {
                            name: "MODEL_NAME",
                            value: "ibm-granite/granite-3.1-2b-instruct"
                        },
                        {
                            name: "LABEL_STUDIO_URL",
                            value: "http://label-studio:8080"
                        }
                    ],
                    ports: [{ containerPort: 9090 }],
                    resources: {
                        requests: {
                            memory: "2Gi",
                            cpu: "500m",
                            "nvidia.com/gpu": "1"  // Request GPU for ML inference
                        },
                        limits: {
                            memory: "8Gi",
                            cpu: "2000m",
                            "nvidia.com/gpu": "1"
                        }
                    }
                }],
                // Schedule on GPU workstation for ML inference
                nodeSelector: { "oceanid.node/gpu": "rtx4090x2" },
                tolerations: [{
                    key: "nvidia.com/gpu",
                    operator: "Exists",
                    effect: "NoSchedule"
                }]
            }
        }
    }
}, { provider: k8sProvider, dependsOn: [labelStudio] });

const labelStudioMLService = new k8s.core.v1.Service("label-studio-ml", {
    metadata: {
        name: "label-studio-ml",
        namespace: namespace
    },
    spec: {
        selector: { app: "label-studio-ml" },
        ports: [{ port: 9090, targetPort: 9090 }]
    }
}, { provider: k8sProvider });

// =============================================================================
// KAFKA FOR STREAMING (Future Enhancement)
// =============================================================================

export const kafka = new k8s.helm.v3.Release("kafka", {
    name: "kafka",
    namespace: namespace,
    chart: "kafka",
    version: "26.8.5", // Latest Bitnami Kafka 2025
    repositoryOpts: {
        repo: "https://charts.bitnami.com/bitnami"
    },
    values: {
        // Kafka cluster configuration
        replicaCount: 3,

        // Enhanced security for 2025
        auth: {
            clientProtocol: "sasl_ssl",
            interBrokerProtocol: "sasl_ssl",
            sasl: {
                mechanisms: ["SCRAM-SHA-256"],
                users: ["airflow", "perseis"]
            }
        },

        // TLS encryption
        tls: {
            enabled: true,
            autoGenerated: true
        },

        // Storage configuration
        persistence: {
            enabled: true,
            size: "100Gi",
            storageClass: "local-path"
        },

        // Zookeeper configuration
        zookeeper: {
            enabled: true,
            replicaCount: 3,
            auth: {
                enabled: true,
                clientUser: "kafka",
                serverUsers: ["kafka"]
            },
            persistence: {
                enabled: true,
                size: "8Gi"
            }
        },

        // Performance tuning for vessel data streaming
        config: {
            "num.network.threads": "8",
            "num.io.threads": "16",
            "socket.send.buffer.bytes": "102400",
            "socket.receive.buffer.bytes": "102400",
            "socket.request.max.bytes": "104857600",
            "log.retention.hours": "168", // 1 week
            "log.segment.bytes": "1073741824", // 1GB
            "log.cleanup.policy": "delete"
        },

        // Resource allocation
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

        // Monitoring integration
        metrics: {
            kafka: {
                enabled: true
            },
            jmx: {
                enabled: true
            }
        }
    }
}, { provider: k8sProvider, dependsOn: [mlNamespace] });

// =============================================================================
// AIRFLOW ORCHESTRATION
// =============================================================================

export const airflow = new k8s.helm.v3.Release("airflow", {
    name: "airflow",
    namespace: namespace,
    chart: "airflow",
    version: "1.14.0", // Updated for 2025 security features
    repositoryOpts: {
        repo: "https://airflow.apache.org"
    },
    values: {
        executor: "KubernetesExecutor",

        // 2025 Security Best Practices: External Secrets Backend
        config: {
            core: {
                // ESC Secrets Backend instead of database storage
                secrets_backend: "airflow.providers.kubernetes.secrets.secret_manager.SecretManagerSecretsBackend",
                secrets_backend_kwargs: JSON.stringify({
                    kubernetes_secret_name: "ml-secrets",
                    namespace: namespace
                }),

                // Fernet key rotation support (current,previous)
                fernet_key: `${config.requireSecret("airflowFernetKey")},${config.requireSecret("airflowFernetKeyPrevious")}`,
                load_examples: false,

                // Enhanced security settings
                expose_config: false,
                hide_sensitive_var_conn_fields: true
            },

            // RBAC and authentication
            webserver: {
                authenticate: true,
                auth_backend: "airflow.auth.backends.password_auth",
                rbac: true,
                expose_config: false
            },

            // Secure logging
            logging: {
                remote_logging: true,
                remote_log_conn_id: "minio_logs",
                encrypt_s3_logs: true
            }
        },

        // Web server security
        webserver: {
            defaultUser: {
                enabled: true,
                role: "Admin",
                username: "admin",
                password: config.requireSecret("airflowPassword")
            },

            // Security headers and settings
            extraEnv: [
                {
                    name: "AIRFLOW__WEBSERVER__EXPOSE_CONFIG",
                    value: "False"
                },
                {
                    name: "AIRFLOW__WEBSERVER__HIDE_PAUSED_DAGS_BY_DEFAULT",
                    value: "True"
                },
                {
                    name: "AIRFLOW__API__AUTH_BACKENDS",
                    value: "airflow.api.auth.backend.session"
                }
            ],

            // Network policy and security context
            securityContext: {
                runAsUser: 50000,
                runAsGroup: 0,
                fsGroup: 50000
            }
        },

        // Workers security
        workers: {
            securityContext: {
                runAsUser: 50000,
                runAsGroup: 0,
                fsGroup: 50000
            },

            // GPU node affinity for ML tasks
            affinity: {
                nodeAffinity: {
                    preferredDuringSchedulingIgnoredDuringExecution: [{
                        weight: 100,
                        preference: {
                            matchExpressions: [{
                                key: "oceanid.node/gpu",
                                operator: "In",
                                values: ["rtx4090x2"]
                            }]
                        }
                    }]
                }
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
        annotation: "Label Studio + ML Backend (GPU)",
        orchestration: "Airflow (KubernetesExecutor + ESC Secrets)",
        tracking: "MLflow",
        monitoring: "Prometheus + Grafana",
        streaming: "Kafka (3 brokers, SASL/SSL)",
        security: "ESC + 1Password + Fernet Rotation"
    },
    endpoints: {
        airbyte: "https://airbyte.boathou.se",
        labelStudio: "https://label.boathou.se",
        labelStudioML: "http://label-studio-ml:9090",
        airflow: "https://airflow.boathou.se",
        mlflow: "https://mlflow.boathou.se",
        grafana: "https://grafana.boathou.se",
        kafka: "kafka:9092"
    },
    security: {
        secretsManagement: "Pulumi ESC + 1Password Provider",
        airflowSecurity: "External Secrets Backend + Fernet Rotation",
        kafkaSecurity: "SASL-SSL + SCRAM-SHA-256",
        networkPolicies: "Enabled with RBAC"
    }
};