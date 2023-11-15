SHELL=/bin/bash

help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

up: ## Up spark containers
	@docker compose up -d

down: ## Delete containers
	@docker compose down

exec-bash: ## Access spark-master container 
	@docker exec -it $(container) /bin/bash

run-job: ## Submit spark
	@docker exec -it spark-master spark-submit /opt/spark/app/src/process_orders.py

bucket-s3: ## Action on the bucket
	@docker exec -it simulator-aws sh /tmp/itau-shop/bucket_s3.sh $(bucket) $(action) $(path)

config: ## Add config for containers
	@mkdir input checkpoint
	@chmod 777 input checkpoint

clean: ## Delete directories
	@rm -rf ./volume/
	@rm -rf checkpoint/ input/