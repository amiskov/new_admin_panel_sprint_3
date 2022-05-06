es-remove-index:
	curl -X DELETE 'http://localhost:9200/_all'
es-create-movies-index:
	curl -XPUT http://127.0.0.1:9200/movies -H 'Content-Type: application/json' --data @resources/es_schema.json
lint:
	flake8 *.py