.PHONY: isort, infra, dep, stop

isort:
	isort parser_dag.py

infra:
	docker-compose -f ./airflow/docker-compose.yaml up

stop:
	docker-compose -f ./airflow/docker-compose.yaml down

dep:
	cp parser_dag.py ./airflow/dags/
