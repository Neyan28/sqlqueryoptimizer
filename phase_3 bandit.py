import random
from collections import defaultdict

class JoinOrderBandit:
    def __init__(self, epsilon=0.2):
        self.epsilon = epsilon
        self.stats = defaultdict(lambda: defaultdict(list))


    def select(self, signature, candidate_orders):
        if random.random() < self.epsilon:
            return random.choice(candidate_orders)

        if signature not in self.stats:
            return random.choice(candidate_orders)

        best_order = None
        best_mean = float("inf")

        for order, times in self.stats[signature].items():
            mean = sum(times) / len(times)
            if mean < best_mean:
                best_mean = mean
                best_order = order

        return best_order or random.choice(candidate_orders)

    def update(self, signature, join_order, exec_time):
        self.stats[signature][tuple(join_order)].append(exec_time)
