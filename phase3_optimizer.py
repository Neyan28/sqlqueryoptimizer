from phase1_runner import (
    extract_tables,
    extract_tables_and_joins,
    generate_join_orders,
    build_from_join_clause_safe,
    rewrite_query_with_order,
    run_explain_and_get_time
)
import random
import psycopg2
from phase2_cache_optimizer import cache_lookup, cache_store, build_query_signature
from phase3_bandit import JoinOrderBandit

bandit = JoinOrderBandit(epsilon=0.3)
def get_valid_join_orders(sql_query):
    tables = extract_tables(sql_query)
    table_aliases, join_conditions = extract_tables_and_joins(sql_query)

    valid_orders = []
    for order in generate_join_orders(tables):
        if build_from_join_clause_safe(order, table_aliases, join_conditions):
            valid_orders.append(order)

    return valid_orders


def phase3_optimize(sql_query, conn):
    signature = build_query_signature(sql_query)

    cached = cache_lookup(signature)
    if cached:
        print("CACHE HIT (Phase-2)")
        return rewrite_query_with_order(
            sql_query, cached["best_join_order"]
        )

    print("CACHE MISS → Phase-3 Bandit")

    valid_orders = get_valid_join_orders(sql_query)
    chosen_order = bandit.select(signature, valid_orders)

    rewritten = rewrite_query_with_order(sql_query, chosen_order)

    cur = conn.cursor()
    exec_time, _ = run_explain_and_get_time(cur, rewritten)
    cur.close()

    bandit.update(signature, chosen_order, exec_time)
    cache_store(signature, chosen_order, exec_time)

    K_TRIALS = 10
    EPSILON = 0.3

bandit = JoinOrderBandit(epsilon=EPSILON)

def measure_mean_exec_time(cur, sql, runs=5):
    times = []
    cur.execute("SET join_collapse_limit = 1")
    for _ in range(runs):
        t, _ = run_explain_and_get_time(cur, sql)
        times.append(t)
    return sum(times) / len(times)

def get_valid_join_orders(sql_query):
    tables = extract_tables(sql_query)
    table_aliases, join_conditions = extract_tables_and_joins(sql_query)

    valid_orders = []
    for order in generate_join_orders(tables):
        if build_from_join_clause_safe(order, table_aliases, join_conditions):
            valid_orders.append(order)

    return valid_orders

def phase3_optimize(sql_query, conn):
    signature = build_query_signature(sql_query)

    # ---- Phase-2 Cache ----
    cached = cache_lookup(signature)
    if cached:
        print("CACHE HIT (Phase-2)")
        return rewrite_query_with_order(sql_query, cached["best_join_order"])

    print("CACHE MISS → Phase-3 (Forced exploration warm-up)")

    valid_orders = get_valid_join_orders(sql_query)
    random.shuffle(valid_orders)

    results = []

    for i in range(min(K_TRIALS, len(valid_orders))):
        join_order = valid_orders[i]
        rewritten = rewrite_query_with_order(sql_query, join_order)

        cur = conn.cursor()
        mean_exec_time = measure_mean_exec_time(cur, rewritten, runs=2)
        cur.close()

        bandit.update(signature, join_order, mean_exec_time)

        print(
            f"Trial {i+1}: Order={join_order}, "
            f"Mean Time={mean_exec_time:.2f} ms"
        )

        results.append((join_order, mean_exec_time))

    best_order, best_time = min(results, key=lambda x: x[1])

    print("BEST ORDER SELECTED:", best_order, "Time:", best_time)

    cache_store(signature, best_order, best_time)

    return rewrite_query_with_order(sql_query, best_order)

conn = psycopg2.connect(
    dbname="tpch",
    user="postgres",
    password="2303",
    host="localhost",
    port=5432
)

sql = """
SELECT
    c.c_custkey,
    c.c_name,
    o.o_orderkey,
    o.o_orderdate,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l
JOIN orders o ON o.o_orderkey = l.l_orderkey
JOIN supplier s ON l.l_suppkey = s.s_suppkey
JOIN nation n ON s.s_nationkey = n.n_nationkey
JOIN customer c ON c.c_custkey = o.o_custkey
WHERE n.n_name = 'GERMANY'
  AND o.o_orderdate >= DATE '1995-01-01'
GROUP BY
    c.c_custkey,
    c.c_name,
    o.o_orderkey,
    o.o_orderdate;
"""

#print("\n PHASE 3: FIRST RUN")
optimized_sql = phase3_optimize(sql, conn)
print(optimized_sql)
print("Enter your SQL query (end with a line containing only ';'):")
lines = []
while True:
    line = input()
    if line.strip() == ";":
        break
    lines.append(line)
sql = "\n".join(lines)

"""print("\n PHASE 3: SECOND RUN")
optimized_sql = phase3_optimize(sql, conn)
print(optimized_sql)"""
print(optimized_sql)

conn.close()
conn.close()
    
