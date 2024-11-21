def publisher(data, context):
 from google.cloud import pubsub_v1
 publish_client = pubsub_v1.PublisherClient()
 topic_name = 'projects/innate-shape-340909/topics/topic'
 output = data['name']
 output = output.encode("utf-8")
 future = publish_client.publish(topic_name, output)
 future.result()
