import json
from google.cloud import pubsub_v1
from flask import jsonify
from google.cloud import datastore


project_id = "jumperdevnew"
topic_id = "ChatInbox-trigger"


client = datastore.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


def chatinbox_trigger(request):
	if request.method == "GET":
		request_args = request.args
		chatinbox_id = request_args['chatinboxid']
		print(chatinbox_id)

		# Fetching ChatInbox data using chatinbox_id
		chat = client.get(client.key("ChatInbox", int(chatinbox_id)))
		
		# Fetching Conversation data using conversation key
		query = client.query(kind='Conversation')
		query.key_filter(chat['conversation'])
		conversation = [i for i in query.fetch()]
		print(conversation[0].key.id)

		# Fetching Broadcastpresets data using fromreference key field
		query = client.query(kind='Broadcastpresets')
		query.key_filter(chat['fromreference'])
		broadcast_presets = [i for i in query.fetch()]
		print(broadcast_presets[0].key.id)

		# Fetching User data using seller key field
		query = client.query(kind='User')
		query.key_filter(chat['seller'])
		user = [i for i in query.fetch()]
		print(user[0].key.id)

		data = {
			"__key__": {
				"namespace": "",
				"app": project_id,
				"path": "",
				"kind": "ChatInbox",
				"name": None,
				"id": chatinbox_id
			},
			"conversation": {
				"namespace": "",
				"app": project_id,
				"path": "",
				"kind": "Conversation",
				"name": None,
				"id": conversation[0].key.id
			},
			"conversationid": chat['conversationid'],
			"created": str(chat['created']),
			"delivered": chat['delivered'],
			"fail_reason": chat['fail_reason'],
			"fromreference": {
				"namespace": "",
				"app": project_id,
				"path": "",
				"kind": "Broadcastpresets",
				"name": broadcast_presets[0]['name'],
				"id": broadcast_presets[0].key.id
			},
			"message": chat['message'],
			"pageid": chat['pageid'],
			"platform": chat['platform'],
			"price": chat['price'],
			"price_model": chat['price_model'],
			"referraldata": chat['referraldata'],
			"referred": chat['referred'],
			"replied": chat['replied'],
			"rule": chat['rule'],
			"source": chat['source'],
			"status": chat['status'],
			"updated": str(chat['updated']),
			"user": {
				"namespace": "",
				"app": project_id,
				"path": "",
				"kind": "ProxyUser",
				"name": user[0]['accountName'],
				"id": user[0].key.id
			},
			"vertex": chat['vertex']
		}

		# Publishing ChatInbox Data to BigQuery
		publisher.publish(topic_path, json.dumps(data, ensure_ascii=False).encode('utf8'))

		return (
			jsonify({
				"success": True
			}), 200, {"Access-Control-Allow-Origin": "*"}
		)
