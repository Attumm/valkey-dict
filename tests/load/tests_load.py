import random
import time
import string
import statistics
from valkey_dict import ValkeyDict

BATCH_SIZE = 1000
OPERATIONS = 100000
SEED = 42
BATCHING = False

random.seed(SEED)

data_types = ["str", "int", "float", "bool", "list", "dict"]


def generate_random_data(data_type):
    if data_type == "str":
        return ''.join(random.choice(string.ascii_letters) for _ in range(10))
    elif data_type == "int":
        return random.randint(1, 100)
    elif data_type == "float":
        return random.uniform(1, 100)
    elif data_type == "bool":
        return random.choice([True, False])
    elif data_type == "list":
        return [random.randint(1, 100) for _ in range(5)]
    else:
        return {f'key{i}': random.randint(1, 100) for i in range(5)}


def main():
    start_total = time.time()
    r = ValkeyDict(namespace="load_test")
    operation_times = []
    batched = BATCHING

    if batched:
        data = []
        for i in range(OPERATIONS):
            key = f"key{i}"
            data_type = random.choice(data_types)
            value = generate_random_data(data_type)
            data.append((key, value))

            if i % BATCH_SIZE == 0:
                with r.pipeline():
                    for key, value in data:
                        start_time = time.time()
                        r[key] = value
                        end_time = time.time()

                        operation_times.append(end_time - start_time)

                print(f"\r{i}/{OPERATIONS} operations completed", end='')
                data = []
        if len(data) > 0:
            for key, value in data:
                start_time = time.time()
                r[key] = value
                end_time = time.time()

                operation_times.append(end_time - start_time)

        print()

    else:
        for i in range(OPERATIONS):
            key = f"key{i}"
            data_type = random.choice(data_types)
            value = generate_random_data(data_type)

            start_time = time.time()
            r[key] = value
            _ = r[key]
            end_time = time.time()

            operation_times.append(end_time - start_time)

            if i % BATCH_SIZE == 0:
                print(f"\r{i}/{OPERATIONS} operations completed", end='')

        print()
    r.clear()

    mean_time = statistics.mean(operation_times)
    min_time = min(operation_times)
    max_time = max(operation_times)
    std_dev = statistics.stdev(operation_times)

    # Adding 'noqa' at the end of lines to suppress the E231 warning due to a bug in pylama with Python 3.12
    print(f"used batching: {batched}, Total operations: {OPERATIONS}, Batch-size: {BATCH_SIZE}")  # noqa: E231
    print(f"Mean time: {mean_time:.6f} s")  # noqa: E231
    print(f"Minimum time: {min_time:.6f} s")  # noqa: E231
    print(f"Maximum time: {max_time:.6f} s")  # noqa: E231
    print(f"Standard deviation: {std_dev:.6f} s")  # noqa: E231

    end_total = time.time()
    total_time = end_total - start_total
    print(f"Total time: {total_time:.6f} s")  # noqa: E231


if __name__ == "__main__":
    main()
