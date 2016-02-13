curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data @WikiStreamEvents-value.json \
    http://localhost:8081/subjects/WikiStreamEvents-value/versions