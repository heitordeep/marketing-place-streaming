SHELL=/bin/bash

help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort


up: ## Up spark containers
	@docker compose up -d

down: ## Delete containers
	@docker compose down

exec-bash: ## Access spark-master container 
	@docker exec -it spark-master /bin/bash

run-job: ## Submit spark
	$(eval SIMULATOR_AWS_IP := $(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' simulator-aws))
	@docker exec -it spark-master spark-submit /opt/spark/app/src/process_orders.py $(SIMULATOR_AWS_IP)

config: ## Add config for containers
	@mkdir checkpoint input output
	@chmod 777 checkpoint input output