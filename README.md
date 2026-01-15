## Running the app locally

### Prerequisites

To run the app locally, you need:
- **Java 17** or later
- **Maven** 3.9+
- **Docker** (for containerized deployment)
- **minikube** and **kubectl** (for Kubernetes deployment)

#### Installing minikube (for Kubernetes)

```bash
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
rm minikube-linux-amd64
```

#### Installing kubectl

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm kubectl
```

### Downloading the graph

In order for the leader to read the graph, you have to download it first.
You can download it [here](https://www.diag.uniroma1.it/challenge9/download.shtml).
You need to put the file with coordinates under `./data/graph.co.gz` and the one
with arcs under `./data/graph.gr.gz` for the Docker containers to work properly.

### Running the Java programs on their own

To build Java apps just run
```
mvn clean install
```
This should create two `.jar` files: `leader/target/leader-1.0-SNAPSHOT.jar` and
`worker/target/worker-1.0-SNAPSHOT.jar`, which you can run with
```
java -jar worker/target/worker-1.0-SNAPSHOT.jar
```
and similarly for the leader. However ***running the apps this way is discouraged***
as it would be quite difficult to make the workers communicate with the leader. Instead,
you can create Docker containers that will make setting everything up quite seamless.

### Running with Docker Compose

To create the Docker containers run
```
chmod +x docker_build.sh
./docker_build.sh
```
after that you can just run
```
docker compose up
```
which will run the leader and the workers. You can observe
their outputs in the terminal.

### Running with Kubernetes (minikube) - Recommended

This is the recommended approach if you plan to deploy to Google Cloud later.

#### 1. Start minikube

```bash
sudo usermod -aG docker $USER && newgrp docker
./minikube_start.sh
```

Or manually:
```bash
minikube start --driver=docker --cpus=4 --memory=4096 --disk-size=20g
```

#### 2. Build images for Kubernetes

```bash
./k8s_build.sh
```

#### 3. Deploy to Kubernetes

```bash
./k8s_deploy.sh
```

This script will:
- Mount your `./data` directory to minikube
- Create the namespace and all Kubernetes resources
- Start 3 workers and then the leader job

#### Useful kubectl commands

```bash
# View all pods
kubectl get pods -n graph-dist

# Watch pod status
kubectl get pods -n graph-dist -w

# View leader logs
kubectl logs -n graph-dist -l app=graph-leader -f

# View worker logs
kubectl logs -n graph-dist worker-0
```

#### Cleaning up

```bash
./k8s_cleanup.sh   # Remove Kubernetes resources
minikube stop      # Stop minikube
```

## Running the app on Google Cloud (GKE)

### Prerequisites

1. Install and configure the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
2. Authenticate: `gcloud auth login`
3. Set your project: `gcloud config set project YOUR_PROJECT_ID`

### Create a GKE cluster

```bash
gcloud container clusters create graph-cluster \
    --zone us-central1-a \
    --num-nodes 3 \
    --machine-type e2-medium
```

### Configure kubectl for GKE

```bash
gcloud container clusters get-credentials graph-cluster --zone us-central1-a
```

### Push images to Google Container Registry

```bash
# Build images locally first
./docker_build.sh

# Tag images for GCR
docker tag graph-worker:v1 gcr.io/YOUR_PROJECT_ID/graph-worker:v1
docker tag graph-leader:v1 gcr.io/YOUR_PROJECT_ID/graph-leader:v1

# Push to GCR
docker push gcr.io/YOUR_PROJECT_ID/graph-worker:v1
docker push gcr.io/YOUR_PROJECT_ID/graph-leader:v1
```

### Deploy to GKE

1. Update image references in `k8s/*.yaml` files:
   - Change `image: graph-worker:v1` to `image: gcr.io/YOUR_PROJECT_ID/graph-worker:v1`
   - Change `image: graph-leader:v1` to `image: gcr.io/YOUR_PROJECT_ID/graph-leader:v1`
   - Change `imagePullPolicy: Never` to `imagePullPolicy: Always`

2. Upload graph data to a GCS bucket or use a different storage solution

3. Apply the manifests:
   ```bash
   kubectl apply -f k8s/namespace.yaml
   kubectl apply -f k8s/configmap.yaml
   kubectl apply -f k8s/worker-statefulset.yaml
   kubectl apply -f k8s/leader-job.yaml
   ```

### Clean up GKE resources

```bash
# Delete the cluster (to avoid charges)
gcloud container clusters delete graph-cluster --zone us-central1-a
```