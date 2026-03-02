NAMESPACE ?= matching-engine
MATCHING_ENGINE_METRICS_PORT ?= 2112
MONITORING_NAMESPACE ?= monitoring
PROMETHEUS_RELEASE ?= prometheus
GRAFANA_RELEASE ?= grafana

# Build images for service
build_kafkainit:
	docker build --no-cache -t local/kafka-init -f src/kafka_init/Dockerfile src/kafka_init/

build_matching_engine:
	docker build --no-cache -t local/matching-engine -f src/matching-engine/Dockerfile .

build_traderpool:
	docker build --no-cache -t local/traderpool -f src/traderpool/Dockerfile .

build: build_kafkainit build_matching_engine build_traderpool

helm:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo add grafana https://grafana.github.io/helm-charts
	helm repo update

clear_helm:
	helm repo remove bitnami
	helm repo remove prometheus-community
	helm repo remove grafana

# Local infra
start_infra:
	kubectl apply -f k8s/namespaces.yaml

start_deps:
	helm upgrade --install bitnami bitnami/kafka --version 31.0.0 -n $(NAMESPACE) --create-namespace -f k8s/helm/values/kafka-values-local.yaml

stop_deps:
	helm uninstall --ignore-not-found bitnami -n $(NAMESPACE)
	kubectl delete --ignore-not-found pvc data-bitnami-kafka-controller-0 -n $(NAMESPACE)

start: start_infra start_deps monitoring-up
	helm install matching-engine ./k8s/helm/chart --namespace $(NAMESPACE)

stop: monitoring-down stop_deps
	helm uninstall --ignore-not-found matching-engine --namespace $(NAMESPACE)

monitoring-up:
	helm upgrade --install $(PROMETHEUS_RELEASE) prometheus-community/prometheus -n $(MONITORING_NAMESPACE) --create-namespace -f k8s/helm/values/prometheus-values-local.yaml
	helm upgrade --install $(GRAFANA_RELEASE) grafana/grafana -n $(MONITORING_NAMESPACE) --create-namespace -f k8s/helm/values/grafana-values-local.yaml

monitoring-down:
	helm uninstall --ignore-not-found $(GRAFANA_RELEASE) -n $(MONITORING_NAMESPACE)
	helm uninstall --ignore-not-found $(PROMETHEUS_RELEASE) -n $(MONITORING_NAMESPACE)

monitoring-grafana:
	kubectl -n $(MONITORING_NAMESPACE) port-forward svc/$(GRAFANA_RELEASE) 3000:80

monitoring-prometheus:
	kubectl -n $(MONITORING_NAMESPACE) port-forward svc/$(PROMETHEUS_RELEASE)-server 9090:80

# Need virtual env or python setup with requirements installed
# 1. test contracts
# 2. test matching-engine
# 3. test traderbook
test:
	cd src/matching-engine/ && go test
	cd src/traderpool/ && go test

ruff:
	uv run ruff format . && uv run ruff check --fix