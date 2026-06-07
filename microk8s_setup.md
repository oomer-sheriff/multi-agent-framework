# MicroK8s Setup Guide for Windows

Since you are running on Windows, Canonical provides a native MicroK8s installer that creates a lightweight, optimized Ubuntu VM behind the scenes using Multipass.

Here is the step-by-step guide to installing MicroK8s and deploying the DMAF multi-agent framework on your system.

## 1. Install MicroK8s

1. Download the latest MicroK8s Windows Installer from Canonical:
   [Download MicroK8s for Windows](https://microk8s.io/docs/install-windows)
2. Run the installer. It will automatically install Multipass and configure the MicroK8s VM.
3. Open a **new** PowerShell window (as Administrator) to refresh your environment variables.
4. Verify the installation is running:
   ```powershell
   microk8s status --wait-ready
   ```

## 2. Enable Required Kubernetes Add-ons

DMAF requires a few specific extensions to function properly, particularly KEDA for autoscaling and a local registry to hold our custom images.

Run the following command:
```powershell
microk8s enable dns storage registry keda
```
*Note: This might take a minute or two to provision the addons.*

## 3. Build and Import Docker Images

MicroK8s runs in its own isolated VM, meaning it cannot see the Docker images you built locally with `docker-compose`. We need to export your local images and import them into MicroK8s.

First, build the images cleanly using `docker-compose` (you've likely already done this):
```powershell
cd D:\project-dma\DMAF
docker-compose build
```

Next, export the generated images from your local Docker daemon and pipe them directly into the MicroK8s container runtime (`ctr`):
```powershell
docker save dmaf-api:latest | microk8s ctr image import -
docker save dmaf-frontend:latest | microk8s ctr image import -
docker save dmaf-mcp-server:latest | microk8s ctr image import -
docker save dmaf-agent-worker:latest | microk8s ctr image import -
docker save dmaf-orchestrator-worker:latest | microk8s ctr image import -
```
*(If the pipeline throws an error in PowerShell, you can alternatively save them to a file first: `docker save -o api.tar dmaf-api:latest` and then run `microk8s ctr image import api.tar`)*

## 4. Apply the DMAF Kubernetes Manifests

Now that the cluster is ready and the images are loaded, you can apply the Infrastructure-as-Code manifests we generated.

```powershell
# 1. Start the core infrastructure (Postgres, Redis, MinIO)
microk8s kubectl apply -f k8s/infrastructure.yaml

# 2. Wait for the databases to boot, then start the apps
microk8s kubectl apply -f k8s/apps.yaml

# 3. Apply the KEDA Autoscaler rule
microk8s kubectl apply -f k8s/keda-scaledobject.yaml
```

## 5. Verify the Deployment

Check that all your pods are running successfully:
```powershell
microk8s kubectl get pods -A
```

Check the KEDA autoscaler to ensure it sees the Redis stream:
```powershell
microk8s kubectl get scaledobject
```

### Accessing the System
To access the frontend or API from your Windows browser, you'll need to port-forward the services from the Kubernetes cluster to your local machine:

```powershell
# Port forward the frontend
microk8s kubectl port-forward svc/frontend 5173:5173

# In a separate PowerShell window, port forward the API
microk8s kubectl port-forward svc/api 8000:8000
```
You can now open `http://localhost:5173` in your browser to interact with the fully distributed DMAF platform!
