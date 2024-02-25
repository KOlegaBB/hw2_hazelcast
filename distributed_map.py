import hazelcast


def main():
    client = hazelcast.HazelcastClient(
        cluster_name="hw2_klym",
        cluster_members=["127.0.0.1:5801"]
    )
    map = client.get_map("map_1").blocking()
    for i in range(1000):
        map.put(i, i)

    client.shutdown()


if __name__ == "__main__":
    main()
