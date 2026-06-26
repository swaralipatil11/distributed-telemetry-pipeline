# Distributed, Fault-Tolerant Telemetry Ingestion Pipeline

An enterprise-grade, high-throughput telemetry ingestion pipeline designed to process real-time system metrics (CPU utilization, memory utilization, status flags, and timestamps) from distributed client machines. 

The architecture decouples telemetry reception, shock-absorbing message streaming, background processing, and time-series data storage using Go, Python, Apache Kafka (KRaft), and TimescaleDB, all deployed inside a single Kubernetes namespace on local Minikube.

---

## System Flow Topology

```
[ Clients ] --(HTTP POST /telemetry)--> [ Go Ingestion Gateway (NodePort: 30080) ]
                                                   |
                                       (Asynchronous Producer)
                                                   v
                                        [ Apache Kafka (KRaft) ]
                                                   |
                                         (Buffered Consumer)
                                                   v
                                     [ Python Processing Worker ]
                                                   |
                                         (Relational Insert)
                                                   v
                                      [ TimescaleDB (Time-Series) ]
```

---

## Repository Directory Structure

```
.
├── ingestion-api/
│   ├── Dockerfile
│   ├── go.mod
│   └── main.go
├── k8s-manifest.yaml
├── processing-worker/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── worker.py
└── README.md
```

---

## Deployment & Verification Playbook

Perform these steps in a PowerShell terminal within the repository root directory.

### Task 1: Mount Local Images into Minikube Registry
Configure your shell to build images directly inside the Minikube Docker daemon context:
```powershell
# Point shell context to Minikube's Docker daemon
& minikube -p minikube docker-env --shell powershell | Invoke-Expression

# Build the Go API Gateway container
docker build -t telemetry-ingestion:v1 ./ingestion-api

# Build the Python Processing Worker container
docker build -t telemetry-worker:v1 ./processing-worker
```

### Task 2: Apply Kubernetes Cluster Resources
Deploys Namespace, Services, Deployments, and routing configs:
```powershell
# Deploy the stack to the cluster
kubectl apply -f k8s-manifest.yaml

# Monitor deployment progress until all pods transition to 'Running'
kubectl get pods -n telemetry-stack -w
```

### Task 3: Initialize Database Schema & Hypertable
Deploy the telemetry tables and TimescaleDB hypertable extensions directly inside the running database pod:
```powershell
# Retrieve the TimescaleDB pod name
$DB_POD = (kubectl get pods -n telemetry-stack -l app=timescaledb -o jsonpath='{.items[0].metadata.name}')

# Execute the SQL schema setup commands inside the container
kubectl exec -it $DB_POD -n telemetry-stack -- psql -U postgres -d telemetry_db -c "
CREATE TABLE IF NOT EXISTS metrics (
    timestamp TIMESTAMPTZ NOT NULL,
    machine_id VARCHAR(50) NOT NULL,
    cpu_utilization DOUBLE PRECISION NOT NULL,
    memory_utilization DOUBLE PRECISION NOT NULL,
    status VARCHAR(20) NOT NULL
);
SELECT create_hypertable('metrics', 'timestamp', if_not_exists => TRUE);
"
```

### Task 4: Expose the NodePort Port Bridge
Expose the HTTP gateway to the host system by mapping Minikube's virtual IP:
```powershell
# Connect the NodePort tunnel to localhost:30080
minikube service telemetry-ingestion-svc --url -n telemetry-stack
```

### Task 5: Launch Traffic Simulation Anomaly Bursts
Send metric payloads using PowerShell to trigger data pipeline processing:
```powershell
# Telemetry event: Healthy state
Invoke-RestMethod -Uri "http://localhost:30080/telemetry" -Method Post -ContentType "application/json" -Body '{"machine_id": "server-alpha", "cpu_utilization": 42.8, "memory_utilization": 58.2, "status": "OK", "timestamp": 1782218400}'

# Telemetry event: Critical threshold trigger
Invoke-RestMethod -Uri "http://localhost:30080/telemetry" -Method Post -ContentType "application/json" -Body '{"machine_id": "server-beta", "cpu_utilization": 98.4, "memory_utilization": 91.1, "status": "CRITICAL", "timestamp": 1782218405}'
```

### Task 6: Run SQL Warehouse Queries
Verify metrics have processed through Kafka, triggered rules, and written to TimescaleDB:
```powershell
# Run SELECT query inside the TimescaleDB pod
kubectl exec -it $DB_POD -n telemetry-stack -- psql -U postgres -d telemetry-db -c "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 10;"
```

---

## Architectural Deep Dive

### Fault Tolerance & Backpressure Shock-Absorption
* **Message Decoupling**: The Go Ingestion Gateway returns `202 Accepted` immediately upon writing the payload into Kafka's queue, preventing spikes in traffic from blocking client requests.
* **Persistent Event Replay**: In the event of a TimescaleDB database maintenance window or network partition, Kafka buffers raw events on disk for up to the configured retention period. Once database service is restored, the Python Worker resumes reading from its last committed partition offset, guaranteeing zero data loss.
* **Robust Handshake Protocol**: The Python processing worker initializes with a backoff retry mechanism (10 attempts spaced by 5 seconds) to handle Kafka and DB startup delays gracefully without failing pod initialization checks.
