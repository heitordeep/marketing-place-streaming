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
	$(eval CASSANDRA_IP := $(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' cassandra-node-1))
	@echo "Endereço IP do contêiner Cassandra: $(CASSANDRA_IP)"
	@docker exec -it spark-master spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,com.github.jnr:jnr-posix:3.0.52 /opt/spark/app/src/process_data.py $(CASSANDRA_IP)

config: ## Add config for containers
	@mkdir checkpoint input 
	@chmod 777 checkpoint 

create-table: ## Create table on cassandra
	@python3 src/create_table.py

	