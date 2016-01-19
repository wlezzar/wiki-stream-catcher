# coding: utf8
from __future__ import unicode_literals

import socketIO_client
import requests
import json
import uuid

class WikiNamespace(socketIO_client.BaseNamespace):

    keys = {u'comment', u'wiki', u'server_script_path', u'timestamp', u'server_url',
            u'log_params', u'user', u'id', u'log_action_comment', u'server_name', u'title',
            u'log_id', u'patrolled', u'bot', u'log_type', u'length', u'log_action',
            u'namespace', u'type', u'minor', u'revision'}

    headers = {'Content-Type': 'application/vnd.kafka.avro.v1+json'}

    @staticmethod
    def format_data(event):
        formatted = dict([(key, "" if not event.has_key(key) else (event[key] if isinstance(event[key], basestring) else str(event[key]))) for key in WikiNamespace.keys])
        formatted[u"event_uuid"] = str(uuid.uuid1())
        return formatted

    def on_change(self, change):
        print(change)
        data = json.dumps({"value_schema_id":1, "records": [{"value":WikiNamespace.format_data(change)}]})
        response = requests.post("http://localhost:8082/topics/WikiStreamEvents", data=data, headers=WikiNamespace.headers)
        print(data)
        print(response.content)

    def on_connect(self):
        self.emit('subscribe', 'commons.wikimedia.org')


socketIO = socketIO_client.SocketIO('stream.wikimedia.org', 80)
socketIO.define(WikiNamespace, '/rc')

socketIO.wait(10)