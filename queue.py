import hazelcast
from threading import Thread


def produce():
    client = hazelcast.HazelcastClient(
        cluster_name="hw2_klym",
        cluster_members=["127.0.0.1:5801"]
    )

    queue = client.get_queue("queue")

    for i in range(100):
        queue.put(i).result()
    queue.put(-1).result()

    client.shutdown()


def consume():
    client = hazelcast.HazelcastClient(
        cluster_name="hw2_klym",
        cluster_members=["127.0.0.1:5801"]
    )

    queue = client.get_queue("queue")

    while True:
        head = queue.take().result()
        if head == -1:
            queue.put(-1).result()
            break
        print(f"Consuming {head}")
    client.shutdown()


if __name__ == "__main__":
    threads = []
    t = Thread(target=produce)
    t.start()
    threads.append(t)
    for _ in range(2):
        t = Thread(target=consume)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    client = hazelcast.HazelcastClient(
        cluster_name="hw2_klym",
        cluster_members=["127.0.0.1:5801"]
    )

    queue = client.get_queue("queue")

    queue.clear().result()

    client.shutdown()
