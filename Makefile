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

create-bucket: ## Create bucket
	@docker exec -it simulator-aws sh /tmp/itau-shop/create-bucket.sh

list-bucket: ## List content on bucket
	@docker exec -it simulator-aws awslocal s3 ls s3://itau-shop/$(path)

move-file-bucket: ## Create bucket
	@docker exec -it simulator-aws sh /tmp/itau-shop/move-file.sh

config: ## Add config for containers
	@mkdir input checkpoint
	@chmod 777 input checkpoint

clean: ## Delete directories
	@rm -rf ./volume/
	@rm -rf checkpoint/ input/