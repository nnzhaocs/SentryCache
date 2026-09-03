#!/usr/bin/env python3
"""Render WarmScale/SentryCache manifests for the 32-node experiment suite."""

from __future__ import annotations

import argparse
from pathlib import Path


SPREAD = """      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              app: {app_label}
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              app: {app_label}
"""


def block_scalar(path: Path, indent: int = 4) -> str:
    prefix = " " * indent
    text = path.read_text(encoding="utf-8")
    return "\n".join(prefix + line if line else prefix for line in text.splitlines())


def render_l2(namespace: str) -> str:
    return f"""---
apiVersion: v1
kind: Namespace
metadata:
  name: {namespace}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: l2-metadata
  namespace: {namespace}
  labels:
    app: l2-metadata
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: l2-metadata
  template:
    metadata:
      labels:
        app: l2-metadata
    spec:
      containers:
        - name: redis
          image: redis:7.2-alpine
          imagePullPolicy: IfNotPresent
          args: ["--maxmemory","64mb","--maxmemory-policy","noeviction","--save","","--appendonly","no","--bind","0.0.0.0","--protected-mode","no"]
          ports:
            - containerPort: 6379
---
apiVersion: v1
kind: Service
metadata:
  name: l2-metadata
  namespace: {namespace}
spec:
  selector:
    app: l2-metadata
  ports:
    - port: 6379
      targetPort: 6379
"""


def render_cache_service(namespace: str, app_path: Path, replicas: int) -> str:
    app_code = block_scalar(app_path, indent=4)
    spread = SPREAD.format(app_label="cache-svc").rstrip()
    return f"""---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cache-svc-code
  namespace: {namespace}
data:
  app.py: |
{app_code}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cache-svc
  namespace: {namespace}
  labels:
    app: cache-svc
    sc-deployment: cache-svc
spec:
  replicas: {replicas}
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 8
      maxUnavailable: 0
  selector:
    matchLabels:
      app: cache-svc
  template:
    metadata:
      labels:
        app: cache-svc
        sc-deployment: cache-svc
        sc-version: v1
    spec:
{spread}
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
            - name: REDIS_HOST
              value: "127.0.0.1"
            - name: REDIS_PORT
              value: "6379"
            - name: BACKEND_MS
              value: "200"
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 2
            failureThreshold: 30
          lifecycle:
            preStop:
              exec:
                command: ["/bin/sh","-c","sleep 25"]
          volumeMounts:
            - name: code
              mountPath: /code
        - name: redis
          image: redis:7.2-alpine
          imagePullPolicy: IfNotPresent
          args: ["--maxmemory","8mb","--maxmemory-policy","allkeys-lfu","--save","","--appendonly","no","--bind","0.0.0.0","--protected-mode","no"]
          ports:
            - containerPort: 6379
              name: redis
          lifecycle:
            preStop:
              exec:
                command: ["/bin/sh","-c","sleep 25"]
      volumes:
        - name: code
          configMap:
            name: cache-svc-code
---
apiVersion: v1
kind: Service
metadata:
  name: cache-svc
  namespace: {namespace}
spec:
  type: ClusterIP
  selector:
    app: cache-svc
  ports:
    - port: 80
      targetPort: 8080
"""


def render_timeline(namespace: str, app_path: Path, replicas: int) -> str:
    app_code = block_scalar(app_path, indent=4)
    spread = SPREAD.format(app_label="timeline").rstrip()
    return f"""---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: shared-cache
  namespace: {namespace}
  labels:
    app: shared-cache
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: shared-cache
  template:
    metadata:
      labels:
        app: shared-cache
    spec:
      containers:
        - name: redis
          image: redis:7.2-alpine
          imagePullPolicy: IfNotPresent
          args: ["--maxmemory","32mb","--maxmemory-policy","allkeys-lfu","--save","","--appendonly","no","--bind","0.0.0.0","--protected-mode","no","--databases","16"]
          ports:
            - containerPort: 6379
---
apiVersion: v1
kind: Service
metadata:
  name: shared-cache
  namespace: {namespace}
spec:
  selector:
    app: shared-cache
  ports:
    - port: 6379
      targetPort: 6379
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: timeline-code
  namespace: {namespace}
data:
  app.py: |
{app_code}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: timeline
  namespace: {namespace}
  labels:
    app: timeline
    sc-deployment: timeline
spec:
  replicas: {replicas}
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 8
      maxUnavailable: 0
  selector:
    matchLabels:
      app: timeline
  template:
    metadata:
      labels:
        app: timeline
        sc-deployment: timeline
        sc-version: v1
    spec:
{spread}
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
            - name: VERSION
              value: "v1"
            - name: REDIS_DB
              value: "0"
            - name: REDIS_HOST
              value: "shared-cache"
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 2
            failureThreshold: 30
          volumeMounts:
            - name: code
              mountPath: /code
      volumes:
        - name: code
          configMap:
            name: timeline-code
---
apiVersion: v1
kind: Service
metadata:
  name: timeline
  namespace: {namespace}
spec:
  selector:
    app: timeline
  ports:
    - port: 80
      targetPort: 8080
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="sc-eval-32")
    parser.add_argument("--replicas", type=int, default=32)
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    args = parser.parse_args()

    root = Path(args.root)
    cache_app = root / "eval" / "exp_5_4_migration" / "app.py"
    timeline_app = root / "eval" / "exp_5_3_rolling" / "app.py"

    print(render_l2(args.namespace))
    print(render_cache_service(args.namespace, cache_app, args.replicas))
    print(render_timeline(args.namespace, timeline_app, args.replicas))


if __name__ == "__main__":
    main()
