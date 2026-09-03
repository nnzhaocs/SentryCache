#!/usr/bin/env bash
# Deploy HotelReservation to sc-eval-hotel namespace, with profile service
# patched to use a per-pod memcached sidecar instead of the shared
# memcached-profile Service. Idempotent — safe to re-run.
set -e
: "${DEATHSTARBENCH_ROOT:?set DEATHSTARBENCH_ROOT to the DeathStarBench checkout}"
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy
NS=sc-eval-hotel
HR=${DEATHSTARBENCH_ROOT}/hotelReservation

kubectl create namespace $NS --dry-run=client -o yaml | kubectl apply -f -

# Apply all standard HR manifests EXCEPT profile (we'll override) and the
# shared memcached-profile (no longer needed, we sidecar it).
for d in consul jaeger frontend geo rate reccomend reserve search user; do
  kubectl apply -n $NS -f $HR/kubernetes/$d/ 2>&1 | tail -3
done

# Apply non-profile parts of the profile dir (the MongoDB):
kubectl apply -n $NS -f $HR/kubernetes/profile/profile-mongodb-deployment.yaml 2>&1 | tail -1
kubectl apply -n $NS -f $HR/kubernetes/profile/profile-mongodb-service.yaml 2>&1 | tail -1
# Skip: profile-deployment.yaml, profile-service.yaml (custom replacements below)
# Skip: memcached-profile-* (per-pod sidecar replaces shared service)

# Custom config.json with localhost memcached for profile.
kubectl create configmap hr-profile-config -n $NS --from-literal=config.json='{
  "consulAddress": "consul:8500",
  "jaegerAddress": "jaeger:6831",
  "FrontendPort": "5000",
  "GeoPort": "8083",
  "GeoMongoAddress": "mongodb-geo:27017",
  "ProfilePort": "8081",
  "ProfileMongoAddress": "mongodb-profile:27017",
  "ProfileMemcAddress": "localhost:11211",
  "ReviewPort": "8088",
  "ReviewMongoAddress": "mongodb-review:27017",
  "ReviewMemcAddress": "memcached-review:11211",
  "AttractionsPort": "8089",
  "AttractionsMongoAddress": "mongodb-attractions:27017",
  "RatePort": "8084",
  "RateMongoAddress": "mongodb-rate:27017",
  "RateMemcAddress": "memcached-rate:11211",
  "RecommendPort": "8085",
  "RecommendMongoAddress": "mongodb-recommendation:27017",
  "ReservePort": "8087",
  "ReserveMongoAddress": "mongodb-reservation:27017",
  "ReserveMemcAddress": "memcached-reserve:11211",
  "SearchPort": "8082",
  "UserPort": "8086",
  "UserMongoAddress": "mongodb-user:27017",
  "KnativeDomainName": ""
}' --dry-run=client -o yaml | kubectl apply -f -

# Profile deployment with per-pod memcached sidecar.
cat <<'EOF' | kubectl apply -n $NS -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: profile
  labels:
    io.kompose.service: profile
    sc-deployment: profile
spec:
  replicas: 3
  selector:
    matchLabels:
      io.kompose.service: profile
  strategy:
    type: RollingUpdate
    rollingUpdate: {maxSurge: 0, maxUnavailable: 1}
  template:
    metadata:
      labels:
        io.kompose.service: profile
        sc-deployment: profile
        sc-version: v1
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - command: ["./profile"]
          image: deathstarbench/hotel-reservation:latest
          imagePullPolicy: IfNotPresent
          name: hotel-reserv-profile
          ports:
            - {containerPort: 8081}
          volumeMounts:
            - {name: cfg, mountPath: /go/src/github.com/harlow/go-micro-services/config.json, subPath: config.json}
            - {name: cfg, mountPath: /config.json, subPath: config.json}
        - name: memcached-sidecar
          image: memcached
          imagePullPolicy: IfNotPresent
          args: ["-m","128","-t","2","-p","11211"]
          ports:
            - {containerPort: 11211}
      volumes:
        - name: cfg
          configMap: {name: hr-profile-config}
---
apiVersion: v1
kind: Service
metadata:
  name: profile
  labels: {io.kompose.service: profile}
spec:
  ports:
    - {name: "8081", port: 8081, targetPort: 8081}
  selector:
    io.kompose.service: profile
EOF

echo "applied; waiting for core services"
for d in consul frontend geo rate reserve search user profile-mongodb mongodb-geo mongodb-profile mongodb-rate mongodb-recommendation mongodb-reservation mongodb-user mongodb-review mongodb-attractions; do
  kubectl rollout status -n $NS deploy/$d --timeout=180s 2>&1 | tail -1 || true
done
kubectl rollout status -n $NS deploy/profile --timeout=240s 2>&1 | tail -1

echo "=== final pod state ==="
kubectl get pods -n $NS 2>&1 | head -30
