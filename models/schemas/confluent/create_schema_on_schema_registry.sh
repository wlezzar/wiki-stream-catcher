BASENAME=$(dirname $0)

curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data @$BASENAME/WikiStreamEvents-value.json \
    http://localhost:8081/subjects/WikiStreamEvents-value/versions