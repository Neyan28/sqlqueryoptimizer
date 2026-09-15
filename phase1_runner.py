#!/usr/bin/env python3
# phase1_runner.py
import re
import itertools
import os
import csv
import json
import hashlib
import datetime
import psycopg2
from pathlib import Path

DB_NAME = "tpch"
DB_USER = "postgres"
DB_PASS = "2303"
DB_HOST = "localhost"
DB_PORT = 5432

QUERY_FOLDER = r"D:\TPC\queries"
OUT_DIR = "phase1_output"
OUTPUT_CSV = os.path.join(OUT_DIR, "tpch_query_times.csv")
RUNS_PER_PERM = 5
MAX_PERMS = None


def extract_tables_and_joins(sql_query):
    q = sql_query.strip()

    table_alias_pattern = r"(from|join)\s+([a-zA-Z0-9_]+)(?:\s+([a-zA-Z0-9_]+))?"
    table_alias_matches = re.findall(table_alias_pattern, q, flags=re.IGNORECASE)

    table_aliases = {}
    alias_to_table = {}

    for _, table, alias in table_alias_matches:
        if not alias:
            alias = table
        table = table.lower()
        alias = alias.lower()
        table_aliases[table] = alias
        alias_to_table[alias] = table

    join_cond_pattern = r"on\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\s*=\s*([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)"
    join_matches = re.findall(join_cond_pattern, q, flags=re.IGNORECASE)

    join_conditions = {}
    for left, right in join_matches:
        left_alias = left.split('.')[0].lower()
        right_alias = right.split('.')[0].lower()

        t1 = alias_to_table.get(left_alias)
        t2 = alias_to_table.get(right_alias)
        if not t1 or not t2:
            continue

        key = frozenset({t1, t2})
        cond_text = f"{left} = {right}"
        join_conditions[key] = cond_text

    return table_aliases, join_conditions

def extract_tables(sql_query):
    q = sql_query.lower()
    pattern = r"(from|join)\s+([a-zA-Z0-9_]+)"
    matches = re.findall(pattern, q)
    tables = [m[1].lower() for m in matches]

    unique_tables = []
    for t in tables:
        if t not in unique_tables:
            unique_tables.append(t)
    return unique_tables

def generate_join_orders(tables):
    return list(itertools.permutations(tables))

def split_select_from(sql_query):
    parts = re.split(r'\bfrom\b', sql_query, flags=re.IGNORECASE, maxsplit=1)
    if len(parts) < 2:
        raise ValueError("Query does not contain FROM")
    select_part = parts[0].strip()
    from_onwards = "from " + parts[1].strip()
    return select_part, from_onwards

def build_from_join_clause_safe(order, table_aliases, join_conditions):
    first_table = order[0].lower()

    if first_table not in table_aliases:
        return None

    clause_lines = [f"FROM {first_table} {table_aliases[first_table]}"]
    used = {first_table}

    for t in order[1:]:
        t = t.lower()
        found = False
        for u in used:
            key = frozenset({t, u})
            if key in join_conditions:
                cond = join_conditions[key]
                clause_lines.append(
                    f"JOIN {t} {table_aliases[t]} ON {cond}"
                )
                used.add(t)
                found = True
                break

        if not found:
            return None

    return "\n".join(clause_lines)

def replace_from_clause_keep_tail(original_query, new_from_join_clause):
    parts = re.split(r'\bfrom\b', original_query, flags=re.IGNORECASE, maxsplit=1)
    if len(parts) < 2:
        raise ValueError("Query does not contain FROM")
    select_part = parts[0].strip()
    from_and_rest = parts[1]

    trailing_split = re.split(r'\b(where|group by|having|order by|limit)\b',
                              from_and_rest, flags=re.IGNORECASE, maxsplit=1)

    if len(trailing_split) == 1:
        tail = ""
    else:
        keyword = trailing_split[1]
        rest_after_keyword = trailing_split[2]
        tail = keyword + " " + rest_after_keyword.strip()

    full_query = select_part.strip() + "\n" + new_from_join_clause.strip()
    if tail:
        full_query += "\n" + tail
    full_query = full_query.strip().rstrip(';') + ';'
    return full_query

def rewrite_query_with_order(original_query, order):
    tables = extract_tables(original_query)
    if set(tables) != set([t.lower() for t in order]):
        raise ValueError("Order tables don't match query tables")

    table_aliases, join_conditions = extract_tables_and_joins(original_query)
    from_join_clause = build_from_join_clause_safe(order, table_aliases, join_conditions)
    if from_join_clause is None:
        return None

    full_query = replace_from_clause_keep_tail(original_query, from_join_clause)
    return full_query

def get_exec_time_from_explain_json(explain_json):

    try:
        if isinstance(explain_json, list) and len(explain_json) > 0 and isinstance(explain_json[0], dict):
            d = explain_json[0]

            if 'Execution Time' in d:
                return float(d['Execution Time'])

            def find_exec(obj):
                if isinstance(obj, dict):
                    for k,v in obj.items():
                        if k == 'Execution Time':
                            return float(v)
                        res = find_exec(v)
                        if res is not None:
                            return res
                elif isinstance(obj, list):
                    for item in obj:
                        res = find_exec(item)
                        if res is not None:
                            return res
                return None
            res = find_exec(d)
            if res is not None:
                return float(res)
    except Exception:
        pass
    raise RuntimeError("Could not parse execution time from EXPLAIN JSON")

def run_explain_and_get_time(cur, sql_text):
    sql = sql_text.strip().rstrip(';')
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)
    row = cur.fetchone()
    explain_json = row[0]
    exec_time_ms = get_exec_time_from_explain_json(explain_json)
    return exec_time_ms, explain_json

def optimize_query_once(original_query, conn, query_name="query", runs_per_perm=RUNS_PER_PERM, max_perms=None, out_dir=OUT_DIR):
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    rewrites_dir = os.path.join(out_dir, "rewrites")
    explains_dir = os.path.join(out_dir, "explains")
    summaries_dir = os.path.join(out_dir, "summaries")
    os.makedirs(rewrites_dir, exist_ok=True)
    os.makedirs(explains_dir, exist_ok=True)
    os.makedirs(summaries_dir, exist_ok=True)

    tables = extract_tables(original_query)
    orders = generate_join_orders(tables)

    valid_orders = []
    for order in orders:
        try:
            if build_from_join_clause_safe(order, *extract_tables_and_joins(original_query)) is not None:
                valid_orders.append(order)
        except Exception:
            continue

    if max_perms is not None:
        valid_orders = valid_orders[:max_perms]

    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["query_name","permutation_id","order","run_id","execution_time_ms","explain_file","timestamp"])

    results = []
    cur = conn.cursor()
    for idx, order in enumerate(valid_orders):
        order_str = ",".join(order)
        perm_id_raw = f"{query_name}_{idx}_{order_str}"
        perm_id = hashlib.sha1(perm_id_raw.encode()).hexdigest()[:12]

        try:
            rewritten = rewrite_query_with_order(original_query, order)
        except ValueError as e:
            print(f"Skipping order (mismatch): {order} -> {e}")
            continue

        if rewritten is None:
            print(f"Skipping invalid order: {order}")
            continue

        rewrite_file = os.path.join(rewrites_dir, f"{query_name}_{perm_id}.sql")
        with open(rewrite_file, "w", encoding="utf-8") as f:
            f.write(rewritten)

        run_times = []
        explain_files = []
        for run in range(1, runs_per_perm + 1):
            ts = datetime.datetime.now().isoformat()
            try:
                exec_time_ms, explain_json = run_explain_and_get_time(cur, rewritten)
            except Exception as e:
                print(f"EXPLAIN failed for {query_name} perm {perm_id} run {run}: {e}")
                exec_time_ms = None
                explain_json = None

            explain_path = os.path.join(explains_dir, f"{query_name}_{perm_id}_run{run}.json")
            with open(explain_path, "w", encoding="utf-8") as ef:
                json.dump(explain_json, ef, default=str, indent=2)

            explain_files.append(explain_path)
            run_times.append(exec_time_ms)

            with open(OUTPUT_CSV, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([query_name, perm_id, order_str, run, exec_time_ms, explain_path, ts])

            print(f"{query_name} perm#{idx} run#{run}: {exec_time_ms} ms")

        valid_execs = [t for t in run_times if t is not None]
        mean_ms = sum(valid_execs)/len(valid_execs) if valid_execs else None
        results.append({
            "perm_id": perm_id,
            "order": order,
            "rewrite_file": rewrite_file,
            "explain_files": explain_files,
            "runs": run_times,
            "mean_ms": mean_ms
        })

    best = None
    for r in results:
        if r["mean_ms"] is None:
            continue
        if best is None or r["mean_ms"] < best["mean_ms"]:
            best = r

    summary = {
        "query_name": query_name,
        "timestamp": datetime.datetime.now().isoformat(),
        "tables": tables,
        "permutation_count": len(results),
        "results": results,
        "best": best
    }
    summary_file = os.path.join(summaries_dir, f"{query_name}_summary.json")
    with open(summary_file, "w", encoding="utf-8") as sf:
        json.dump(summary, sf, default=str, indent=2)

    cur.close()
    return summary

