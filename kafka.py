import datetime
from confluent_kafka import TopicPartition, Consumer
import uuid

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': uuid.uuid1(),
    # 'auto.offset.reset': 'earliest'
})

# c.subscribe(['test_topic'])

flag = True
while flag:
    now = int(datetime.datetime.now().timestamp())
    # print(datetime.datetime.now().timestamp())
    now_obj = datetime.datetime.fromtimestamp(now)
    yesterday = (int(now) - 16600)
    print('yesterday ts', yesterday)
    yesterday_obj = datetime.datetime.fromtimestamp(yesterday)
    print(now_obj, yesterday_obj)

    # creating a list
    topic_partitons_to_search = list(map(lambda p: TopicPartition('test_topic', p, int(yesterday*1000)), range(0, 1)))

    print("Searching for offsets with %s" % topic_partitons_to_search)
    offsets = c.offsets_for_times(topic_partitons_to_search, timeout=1.0)

    print("offsets_for_times results: %s" % offsets)
    msg = c.poll(1.0)

    count = 0
    for x in offsets:
        c.seek(x)
        count += 1
    print(count)
    flag = False
