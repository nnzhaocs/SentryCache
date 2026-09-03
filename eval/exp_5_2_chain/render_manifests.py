"""Render the ChainApp k8s manifests for a given DEPTH.

Each layer is a Deployment + Service with a per-pod redis sidecar (8 MB
allkeys-lfu, same as cache-svc in §5.4). All N layers live in
namespace sc-eval-chain so we don't pollute sc-eval. The ConfigMap
chain-code carries app.py mounted into every container.

Output to stdout; caller pipes to kubectl apply -f -.
"""
import sys

DEPTH = int(sys.argv[1]) if len(sys.argv) > 1 else 1
NS = "sc-eval-chain"

print(f"""---
apiVersion: v1
kind: Namespace
metadata: {{name: {NS}}}
""")

for layer in range(1, DEPTH + 1):
    name = f"chain-layer-{layer}"
    print(f"""---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {NS}
  labels: {{app: {name}, sc-deployment: {name}, layer: "{layer}"}}
spec:
  replicas: 3
  strategy: {{type: RollingUpdate, rollingUpdate: {{maxSurge: 0, maxUnavailable: 1}}}}
  selector: {{matchLabels: {{app: {name}}}}}
  template:
    metadata:
      labels: {{app: {name}, sc-deployment: {name}, sc-version: v1, layer: "{layer}"}}
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: app
          image: python:3.11-slim
          imagePullPolicy: IfNotPresent
          command: ["/bin/sh","-c"]
          args:
            - |
              pip install --quiet --no-cache-dir flask redis 2>/dev/null
              exec python3 /code/app.py
          env:
            - {{name: LAYER, value: "{layer}"}}
            - {{name: DEPTH, value: "{DEPTH}"}}
            - {{name: REDIS_HOST, value: "127.0.0.1"}}
            - {{name: REDIS_PORT, value: "6379"}}
            - {{name: BACKEND_MS, value: "200"}}
            - {{name: NS_DOMAIN, value: "{NS}.svc.cluster.local"}}
            - {{name: POD_NAME, valueFrom: {{fieldRef: {{fieldPath: metadata.name}}}}}}
          ports: [{{containerPort: 8080}}]
          readinessProbe:
            httpGet: {{path: /healthz, port: 8080}}
            initialDelaySeconds: 5
            periodSeconds: 2
            failureThreshold: 30
          volumeMounts: [{{name: code, mountPath: /code}}]
        - name: redis
          image: redis:7.2-alpine
          imagePullPolicy: IfNotPresent
          args: ["--maxmemory","8mb","--maxmemory-policy","allkeys-lfu","--save","","--appendonly","no","--bind","0.0.0.0","--protected-mode","no"]
          ports: [{{containerPort: 6379}}]
      volumes:
        - {{name: code, configMap: {{name: chain-code}}}}
---
apiVersion: v1
kind: Service
metadata: {{name: {name}, namespace: {NS}}}
spec:
  type: ClusterIP
  selector: {{app: {name}}}
  ports: [{{port: 80, targetPort: 8080}}]
""")
