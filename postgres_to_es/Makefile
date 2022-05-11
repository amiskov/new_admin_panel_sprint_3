es-remove-index:
	curl -X DELETE 'http://localhost:9200/_all'
es-create-movies-index:
	curl -XPUT http://127.0.0.1:9200/movies -H 'Content-Type: application/json' --data @resources/es_schema.json
lint:
	flake8 *.py
run-elastic:
	docker run -p 9200:9200 \
	-e "http.cors.enabled=true" \
	-e "http.cors.allow-origin=https://app.elasticvue.com" \
	-e "discovery.type=single-node" \
	docker.elastic.co/elasticsearch/elasticsearch:7.7.0
run:
	python main.py
