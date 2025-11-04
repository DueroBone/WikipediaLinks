import ast
import pickle
import time

startTime = time.perf_counter_ns()
i = 0
links_list = []
with open("enwiki_links.pkl", "r") as f:
    for line in f:
        if not line.strip():
            continue
        bdata = ast.literal_eval(line.strip())
        links_list.append(pickle.loads(bdata))
        i += 1

print(
    f"Loaded {i} pages in {(time.perf_counter_ns() - startTime) / 1_000_000_000:.2f} seconds"
)

pickle.dump(links_list, open("enwiki_links_list.pkl", "wb"))

while True:
    pass
