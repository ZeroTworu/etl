.PHONY: isort, infra, dep, stop

isort:
	isort ./parser_dag/

infra:
	docker-compose -f ./airflow/docker-compose.yaml up

stop:
	docker-compose -f ./airflow/docker-compose.yaml down

dep:
	cp -R ./parser_dag/ ./airflow/dags/
