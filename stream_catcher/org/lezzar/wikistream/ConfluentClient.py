import requests
import json

class ConfluentClient:

    headers = {'Content-Type': 'application/vnd.kafka.avro.v1+json'}


    def __init__(self, schema_registry_urls, kafka_rest_urls):
        self.schema_registry_urls = schema_registry_urls
        self.kafka_rest_urls = kafka_rest_urls

        self.last_working_kafka_rest_url = self.kafka_rest_urls[0]
        self.last_working_schema_reg_url = self.schema_registry_urls[0]

        self.topic_to_schema_id_mapping = dict()


    def get_schema_id_for(self,topic):
        if self.topic_to_schema_id_mapping.has_key(topic):
            return self.topic_to_schema_id_mapping[topic]

        schema_info = requests.get(self.last_working_schema_reg_url+"/subjects/"+topic+"-value/versions/latest")
        schema_info.raise_for_status()
        schema_id = schema_info.json()["id"]
        self.topic_to_schema_id_mapping[topic] = schema_id
        return schema_id


    def send(self, data, topic):
        schema_id = self.get_schema_id_for(topic)
        request_data = json.dumps({"value_schema_id":schema_id, "records": [{"value":data}]})
        response = requests.post(self.last_working_kafka_rest_url+"/topics/"+topic, data=request_data, headers=ConfluentClient.headers)
        return response

