from google.cloud import pubsub_v1
from google.cloud import storage
topic_name = 'projects/innate-shape-340909/topics/topic'
subscription_name = 'projects/innate-shape-340909/subscriptions/sub'
def count_lines(p):
 print(p.data)
 client = storage.Client()
 bucket = client.get_bucket('ch18b013')
 blob = bucket.get_blob(p.data.decode('utf-8'))
 x = blob.download_as_string()
 x = x.decode('utf-8')
 print('Number of lines: ', len(x.split('\n')))
 p.ack()
subscriber = pubsub_v1.SubscriberClient()
future = subscriber.subscribe(subscription_name, count_lines)
try:
 future.result()
except KeyboardInterrupt:
 future.cancel()


