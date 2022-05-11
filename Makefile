up:
	docker-compose up --build -d
down:
	docker-compose down -v
view-elastic:
	open https://app.elasticvue.com/cluster/0/search
etl-logs:
	docker exec -it etl_pipeline tail -f logs/es.log
