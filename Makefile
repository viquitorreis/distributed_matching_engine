CLUSTER_NAME := matching-engine
IMAGE := engine:local
DOCKERFILE := infra/docker/Dockerfile
MANIFEST := infra/k8s/engine-statefulset.yaml

.PHONY: setup build load apply run restart teardown logs logs-all status attach-0 attach-1 attach-2 kill-leader

## setup creates the kind cluster (this is only needed once)
setup:
	@kind create cluster --name $(CLUSTER_NAME)

## build builds the local Docker image
build:
	@docker build -t $(IMAGE) -f $(DOCKERFILE) .

## load loads the built image into the kind cluster
load:
	@kind load docker-image $(IMAGE) --name $(CLUSTER_NAME)

## apply applies the k8s manifest (StatefulSet + headless Service)
apply:
	@kubectl apply -f $(MANIFEST)

## run: full pipeline build, load into kind, apply manifest, then watch pods come up
run: build load apply
	@kubectl get pods -w

## restart: rebuilds the image, reloads it, and force-deletes all 3 pods so
## they get recreated with the fresh image (no need to reapply the manifest)
restart: build load
	@kubectl delete pod engine-0 engine-1 engine-2 --grace-period=0 --force --ignore-not-found
	@kubectl get pods -w

## teardown: deletes the kind cluster entirely
teardown:
	@kind delete cluster --name $(CLUSTER_NAME)

## status: quick snapshot of pod state, no watch
status:
	@kubectl get pods

## logs: tail logs from a single pod usage: make logs POD=engine-0
logs:
	@kubectl logs -f $(POD)

## logs-all: dumps current logs from all 3 pods, one after another
logs-all:
	@for p in engine-0 engine-1 engine-2; do \
		echo "=== $$p ==="; \
		kubectl logs $$p; \
		echo; \
	done

## attach-0/1/2: attaches to a running pod's CLI (order/cancel commands)
attach-0:
	@kubectl attach -it engine-0
attach-1:
	@kubectl attach -it engine-1
attach-2:
	@kubectl attach -it engine-2

## kill-leader: usage make kill-leader POD=engine-0 (whichever pod's logs
## show "became leader" most recently); StatefulSet recreates it automatically
kill-leader:
	@kubectl delete pod $(POD) --grace-period=0 --force