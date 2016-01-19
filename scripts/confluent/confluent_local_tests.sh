# launch confluent platform
/opt/modules/confluent/current/bin/zookeeper-server-start /opt/modules/confluent/common/etc/kafka/zookeeper.properties
/opt/modules/confluent/current/bin/kafka-server-start /opt/modules/confluent/common/etc/kafka/server.properties
/opt/modules/confluent/current/bin/schema-registry-start /opt/modules/confluent/common/etc/schema-registry/schema-registry.properties
/opt/modules/confluent/current/bin/kafka-rest-start /opt/modules/confluent/common/etc/kafka-rest/kafka-rest.properties


# Register the WikiStreamEvent schema
curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data @/home/wlezzar/MyFiles/projects/wiki_stream_catcher/models/schemas/wiki_stream_event_schema.json \
    http://localhost:8081/subjects/wiki-stream-event/versions
    
# get schema
curl -X GET http://localhost:8081/subjects/ # get schema lists
curl -X GET -i http://localhost:8081/subjects/wiki-stream-event/versions # get the schema versions of a subject
curl -X GET -i http://localhost:8081/schemas/ids/1 # get the schema by id

# Create topic
/opt/modules/confluent/current/bin/kafka-topics --create --topic WikiStreamEvents --config retention.ms=3600000 --partitions 3 --zookeeper localhost:2181 --replication 1
/opt/modules/confluent/current/bin/kafka-topics --describe --zookeeper localhost:2181


# Insert a message
curl -X POST -H "Content-Type: application/vnd.kafka.avro.v1+json" --data '{"value_schema_id":1,"records":[{"value":{"multiposting_uid":{"int":1},"client_reference":{"string":"AM-1234"},"multiposting_client_id":{"int":96},"title":{"string":"AccountManager"},"job_description":{"string":"Thisisagreatjob!"},"job_profile":{"string":"Wealookingforastronglymotivatedperson."},"application_mode":{"string":"url"},"application_apply_url":{"string":"http:\/\/mycompany.com\/jobs\/AM-1234"},"application_apply_email":{"string":"am-1234@jobs.mycompany.com"},"location_country":{"string":"FR"},"location_city":{"string":"Paris"},"location_postal_code":{"string":"75009"},"company_name":{"string":"MyCompany"},"company_description":{"string":"Thisisthebestcompanyever."},"company_location_country":{"string":"FR"},"company_location_city":{"string":"Paris"},"company_location_postal_code":{"string":"75009"},"company_contact_first_name":{"string":"John"},"company_contact_last_name":{"string":"Doe"},"company_contact_email":{"string":"john.doe@mycompany.com"},"fields_industry":{"int":1},"fields_occupation":{"int":1},"fields_contract":{"int":1},"fields_job_type":{"int":1},"fields_start_date":{"string":"2013-01-01"},"fields_education":{"int":1},"fields_experience":{"int":1},"fields_job_duration":{"string":"10;12"},"fields_salary":{"string":"30000;35000"},"stream_id":1,"store_id":1,"stream_step":1,"stream_action":"new","reception_time":1449673301,"provider":"multiposting","company":"lbc","vertical":"job","status":"received","acc":"acc1"}}]}' "http://localhost:8082/topics/lbc.job.multiposting.input"

curl -X POST -H "Content-Type: application/vnd.kafka.avro.v1+json" --data '{"records": [{"value": {"comment": {"string":"test"}}}], "value_schema_id": 1}' "http://localhost:8082/topics/WikiStreamEvents"
