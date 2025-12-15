# sample_queries.py
from verifier import compare_query_results
from pathlib import Path

setup_sql = Path("sample_setup.sql").read_text()

# Reference solution: total order amount per customer with > 200
reference = """
SELECT c.customer_id, c.name, SUM(o.amount) as total_amount
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name
HAVING SUM(o.amount) > 200
ORDER BY total_amount DESC
"""

# Student attempt (two common variants)
student_variant1 = """
SELECT c.customer_id, c.name, SUM(amount) as total_amount
FROM customers c, orders o
WHERE o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name
HAVING SUM(amount) > 200
ORDER BY total_amount DESC
"""

student_variant2 = """
SELECT c.customer_id, c.name, SUM(o.amount) total_amount
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name
HAVING SUM(o.amount) > 200
ORDER BY total_amount DESC
"""

for i, student in enumerate([student_variant1, student_variant2], start=1):
    out = compare_query_results(student, reference, setup_sql=setup_sql)
    print(f"Variant {i}: equal={out['equal']}, student_success={out['student']['success']}, reference_success={out['reference']['success']}")
    if not out['equal']:
        print("Student rows:", out['student']['rows'])
        print("Reference rows:", out['reference']['rows'])
    print("-----")
