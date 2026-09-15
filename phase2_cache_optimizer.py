import re
import psycopg2
from phase1_runner import optimize_query_once, rewrite_query_with_order


redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

def extract_tables(sql):
    matches = re.findall(r"(from|join)\s+([a-zA-Z0-9_]+)", sql.lower())
    return sorted(set(m[1] for m in matches))

def build_query_signature(sql):
    tables = extract_tables(sql)
    structure = "-".join(tables)
    return hashlib.sha256(structure.encode()).hexdigest()

def cache_lookup(signature):
    data = redis_client.get(signature)
    return json.loads(data) if data else None

def cache_store(signature, best_order, exec_time):
def cache_store(signature, best_order, exec_time,expire=60):
    redis_client.set(signature, json.dumps({
        "best_join_order": best_order,
        "mean_exec_time": exec_time,
        "source": "phase1_oracle"
    }))
    }), ex=expire)

def phase2_optimize_query(sql_query, conn):
    signature = build_query_signature(sql_query)
    cached = cache_lookup(signature)
    if cached:
        print("CACHE HIT (Phase-2)")
        rewritten = rewrite_query_with_order(
            sql_query,
            cached["best_join_order"]
        )
        return rewritten, cached["best_join_order"], cached["mean_exec_time"]


    print("CACHE MISS → Calling Phase-1 Oracle")

    summary = optimize_query_once(
        original_query=sql_query,
        conn=conn,
        query_name="runtime_query",
        runs_per_perm=5
    )

    best_order = summary["best"]["order"]
   best_time = summary["best"]["mean_ms"]

    cache_store(signature, best_order, best_time)

    rewritten = rewrite_query_with_order(sql_query, best_order)
    return rewritten, best_order, best_time

    if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname="tpch",
        user="postgres",
        password="2303",
        host="localhost",
        port=5432
    )
    sql_query = 
    SELECT c.c_custkey, o.o_orderkey
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l ON l.l_orderkey = o.o_orderkey;
    

    rewritten_sql, order, time_ms = phase2_optimize_query(sql_query, conn)

    print("\nFINAL PHASE-2 RESULT")
    print("Join Order:", order)
    print("Mean Exec Time:", time_ms)
    print(rewritten_sql)
    conn.close()"""  
    

  
