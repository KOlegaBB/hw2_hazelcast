from threading import Thread
import hazelcast


def increment_value():
    client = hazelcast.HazelcastClient(
        cluster_name="hw2_klym",
        cluster_members=["127.0.0.1:5801"]
    )
    map = client.get_map("map_3").blocking()
    map.put_if_absent("key", 0)
    for _ in range(10000):
        map.lock("key")
        try:
            current_value = map.get("key")
            new_value = current_value + 1
            map.put("key", new_value)
        finally:
            map.unlock("key")
    client.shutdown()


if __name__ == "__main__":
    threads = []
    for _ in range(3):
        t = Thread(target=increment_value)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
