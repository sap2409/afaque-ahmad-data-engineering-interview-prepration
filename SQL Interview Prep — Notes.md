# SQL Interview Prep — Notes from Two Videos

Sep 30, 2026 · @Sunil Patil

# Part 1 — 37 hospital database questions (sql-practice.com)

## Overview

The video solves every Medium (26) and Hard (11) question on the free **sql-practice.com** hospital database in about 2 hours, live and unrehearsed. No install, login or data import is needed — everything runs in the browser.

- **Where:** sql-practice.com → *View all questions*. Levels are Easy, Medium and Hard; the video skips the \~20 Easy ones.
- **Run a query:** press **Ctrl + Enter**. Every run is auto-validated against the expected output shown under the question.
- **Validation checks data, not headers:** column aliases don't matter, but column count, row order (when asked) and exact formatting (e.g. a `%` sign) do.
- **Dialect:** the site accepts `YEAR()`, `MONTH()`, `DAY()`, `CONCAT()`, `||`, `LIMIT` and window functions like `LAG()`. In SQL Server you would write `TOP 1` instead of `LIMIT 1`.
- **Tip from the video:** use `SELECT * FROM <table>` often to look at the data before writing the real query.

## Hospital database schema

Four tables linked by three joins — memorise these and almost every question becomes "join, then filter or group".

&#91;embedded content: hospital schema · 4 tables, 3 joins\]

A patient can have many admissions, so joining `patients` to `admissions` can repeat a patient. That matters in Hard Q4 (duplicates) and Hard Q5 (cost per admission).

```sql
-- 1. patients ↔ admissions
FROM patients p
JOIN admissions a      ON p.patient_id = a.patient_id
-- 2. patients ↔ province_names
JOIN province_names pn ON p.province_id = pn.province_id
-- 3. admissions ↔ doctors
JOIN doctors d         ON a.attending_doctor_id = d.doctor_id
```

Tip: keep these joins in a commented block in the editor and paste them in when needed. The order of columns in an `ON` condition doesn't matter.

## Medium questions 1–9

These cover DISTINCT, GROUP BY + HAVING, LIKE patterns, simple joins and conditional counting.

### M1. Unique birth years, ascending

`DISTINCT` removes duplicate years; wrap the date in `YEAR()`.

```sql
SELECT DISTINCT YEAR(birth_date) AS birth_year
FROM patients
ORDER BY birth_year;
```

### M2. First names that occur only once

Group by the name, then filter the aggregate with `HAVING` (not `WHERE`).

```sql
SELECT first_name
FROM patients
GROUP BY first_name
HAVING COUNT(*) = 1;
```

### M3. First name starts and ends with "s", at least 6 characters

`LOWER()` guards against mixed case ("S" vs "s"). `%` = any number of characters.

```sql
SELECT patient_id, first_name
FROM patients
WHERE LENGTH(first_name) >= 6
  AND LOWER(first_name) LIKE 's%s';
```

### M4. Patients diagnosed with Dementia

Diagnosis lives in `admissions`, names in `patients` → join. Use table aliases so it's clear where each column comes from.

```sql
SELECT p.patient_id, p.first_name, p.last_name
FROM admissions a
JOIN patients p ON p.patient_id = a.patient_id
WHERE a.diagnosis = 'Dementia';
```

### M5. First names ordered by length, then alphabetically

The second sort key breaks ties (e.g. "Donald" before "Mickey", both 6 letters).

```sql
SELECT first_name
FROM patients
ORDER BY LENGTH(first_name), first_name;
```

### M6. Male and female counts in one row

Conditional aggregation: `COUNT` ignores NULLs, so return an id for a match and NULL otherwise. `SUM(CASE … THEN 1 ELSE 0 END)` works equally well.

```sql
SELECT
  COUNT(CASE WHEN gender = 'M' THEN patient_id END) AS male_count,
  COUNT(CASE WHEN gender = 'F' THEN patient_id END) AS female_count
FROM patients;
```

### M7. Allergic to Penicillin or Morphine

`IN` replaces a chain of `OR`s; sort by three keys in order.

```sql
SELECT first_name, last_name, allergies
FROM patients
WHERE allergies IN ('Penicillin', 'Morphine')
ORDER BY allergies, first_name, last_name;
```

### M8. Patients admitted more than once for the same diagnosis

Group by *both* columns; a count above 1 means a repeat admission for that diagnosis. The count can go in `HAVING` without being selected.

```sql
SELECT patient_id, diagnosis
FROM admissions
GROUP BY patient_id, diagnosis
HAVING COUNT(*) > 1;
```

### M9. Patients per city, most to least, then city name

`ORDER BY` runs after `SELECT`, so it can use the alias. (The instructor's first attempt failed only because `ORDER BY` itself was missing.)

```sql
SELECT city, COUNT(*) AS num_patients
FROM patients
GROUP BY city
ORDER BY num_patients DESC, city ASC;
```

## Medium questions 10–18

These add UNION, date ranges, string functions, `MAX − MIN`, `LIMIT` and compound `AND`/`OR` filters.

### M10. Everyone who is a patient or a doctor, with a role

Stack two tables with `UNION ALL` and hard-code the role as a literal column. Both halves need the same number and order of columns.

```sql
SELECT first_name, last_name, 'Patient' AS role FROM patients
UNION ALL
SELECT first_name, last_name, 'Doctor'  FROM doctors;
```

### M11. Allergies by popularity, no NULLs

Popularity = number of patients with that allergy. The instructor first forgot `GROUP BY` and got one row with no error — some engines silently allow ungrouped columns, so always check.

```sql
SELECT allergies, COUNT(*) AS popularity
FROM patients
WHERE allergies IS NOT NULL
GROUP BY allergies
ORDER BY popularity DESC;
```

### M12. Born in the 1970s, earliest first

A decade is 1970 up to but **not including** 1980.

```sql
SELECT first_name, last_name, birth_date
FROM patients
WHERE YEAR(birth_date) >= 1970 AND YEAR(birth_date) < 1980
ORDER BY birth_date;
```

### M13. "LASTNAME,firstname" in one column

`UPPER` / `LOWER` + `CONCAT` (or `||`). Sort by the original first name, descending.

```sql
SELECT CONCAT(UPPER(last_name), ',', LOWER(first_name)) AS full_name
FROM patients
ORDER BY first_name DESC;
```

### M14. Provinces whose total patient height ≥ 7,000

`province_id` is already in `patients`, so no join is needed. `HAVING` runs before `SELECT`, so repeat `SUM(height)` there instead of the alias.

```sql
SELECT province_id, SUM(height) AS total_height
FROM patients
GROUP BY province_id
HAVING SUM(height) >= 7000;
```

### M15. Weight range for patients with last name Maroni

Filter first, then subtract the aggregates. Result: 112 − 41 = 71.

```sql
SELECT MAX(weight) - MIN(weight) AS weight_delta
FROM patients
WHERE last_name = 'Maroni';
```

### M16. Admissions per day of the month (1–31)

`DAY()` collapses all months and years onto 1–31 (the 11th has the most: 184). No join needed.

```sql
SELECT DAY(admission_date) AS day_number, COUNT(*) AS total_admissions
FROM admissions
GROUP BY day_number
ORDER BY total_admissions DESC;
```

### M17. All columns of patient 542's most recent admission

Sort newest first, keep one row. SQL Server uses `SELECT TOP 1` instead of `LIMIT 1`.

```sql
SELECT *
FROM admissions
WHERE patient_id = 542
ORDER BY admission_date DESC
LIMIT 1;
```

### M18. Admissions matching either of two criteria

Criterion 1: odd `patient_id` **and** doctor 1, 5 or 19. Criterion 2: doctor id contains a "2" **and** `patient_id` is 3 characters. `% 2 = 1` tests for odd; brackets keep the `OR` correct.

```sql
SELECT patient_id, attending_doctor_id, diagnosis
FROM admissions
WHERE (patient_id % 2 = 1 AND attending_doctor_id IN (1, 5, 19))
   OR (attending_doctor_id LIKE '%2%' AND LENGTH(patient_id) = 3);
```

## Medium questions 19–26

These bring in doctor joins, three-table joins, anti-joins and aggregating an aggregate with a CTE.

### M19. Total admissions per doctor

Join admissions to doctors, group by the doctor's name, count rows.

```sql
SELECT d.first_name, d.last_name, COUNT(*) AS admissions_total
FROM admissions a
JOIN doctors d ON a.attending_doctor_id = d.doctor_id
GROUP BY d.first_name, d.last_name;
```

### M20. Each doctor's id, full name, first and last admission date

`MIN` / `MAX` on a date give the earliest / latest. Every non-aggregated column must be in `GROUP BY`.

```sql
SELECT d.doctor_id,
       CONCAT(d.first_name, ' ', d.last_name) AS full_name,
       MIN(a.admission_date) AS first_admission,
       MAX(a.admission_date) AS last_admission
FROM admissions a
JOIN doctors d ON a.attending_doctor_id = d.doctor_id
GROUP BY d.doctor_id, full_name;
```

### M21. Patients per province (by name), descending

The name is only in `province_names`, so join this time.

```sql
SELECT pn.province_name, COUNT(*) AS patient_count
FROM patients p
JOIN province_names pn ON p.province_id = pn.province_id
GROUP BY pn.province_name
ORDER BY patient_count DESC;
```

### M22. Each admission: patient name, diagnosis, doctor name

Three tables, two joins. `||` concatenates just like `CONCAT`.

```sql
SELECT p.first_name || ' ' || p.last_name AS patient_name,
       a.diagnosis,
       d.first_name || ' ' || d.last_name AS doctor_name
FROM patients p
JOIN admissions a ON p.patient_id = a.patient_id
JOIN doctors d    ON a.attending_doctor_id = d.doctor_id;
```

### M23. Duplicate patients (same first and last name)

Group by both names; more than one row = duplicate.

```sql
SELECT first_name, last_name, COUNT(*) AS num_of_duplicates
FROM patients
GROUP BY first_name, last_name
HAVING COUNT(*) > 1;
```

### M24. Full name, height in feet, weight in pounds, birth date, full gender

Feet = cm ÷ 30.48 (1 decimal). Pounds = kg × 2.205 (0 decimals). `CASE` expands M/F.

```sql
SELECT CONCAT(first_name, ' ', last_name) AS patient_name,
       ROUND(height / 30.48, 1) AS height_ft,
       ROUND(weight * 2.205, 0) AS weight_lbs,
       birth_date,
       CASE WHEN gender = 'M' THEN 'MALE' ELSE 'FEMALE' END AS gender_type
FROM patients;
```

### M25. Patients with no admissions

Two ways. `NOT IN` a subquery, or a `LEFT JOIN` anti-join: unmatched rows have NULL in every right-hand column.

```sql
-- Option 1: subquery
SELECT patient_id, first_name, last_name
FROM patients
WHERE patient_id NOT IN (SELECT patient_id FROM admissions);

-- Option 2: LEFT JOIN + IS NULL
SELECT p.patient_id, p.first_name, p.last_name
FROM patients p
LEFT JOIN admissions a ON p.patient_id = a.patient_id
WHERE a.patient_id IS NULL;
```

### M26. Max, min and average admissions per day

Aggregate twice: count per date in a CTE, then take `MAX` / `MIN` / `AVG` of those counts (max 30, min 4). Round the average to 2 decimals.

```sql
WITH daily AS (
  SELECT admission_date, COUNT(*) AS visits
  FROM admissions
  GROUP BY admission_date
)
SELECT MAX(visits) AS max_visits,
       MIN(visits) AS min_visits,
       ROUND(AVG(visits), 2) AS avg_visits
FROM daily;
```

## Hard questions 1–6

The Hard set mostly tests arithmetic tricks, integer division, and knowing which table's grain you are counting.

### H1. Patients per weight group of 10 kg, heaviest group first

Bucket trick: divide by 10, `FLOOR`, multiply by 10. So 76 → 7.6 → 7 → 70 and 104 → 10.4 → 10 → 100.

```sql
SELECT COUNT(*) AS patients_in_group,
       FLOOR(weight / 10) * 10 AS weight_group
FROM patients
GROUP BY weight_group
ORDER BY weight_group DESC;
```

### H2. Flag obese patients (BMI ≥ 30) as 1 or 0

BMI = kg ÷ m², and height is in cm, so divide by **100.0**. Two traps from the video: integer ÷ integer returns 0, and the threshold is 30, not 0.

```sql
SELECT patient_id, weight, height,
       CASE WHEN weight / ((height / 100.0) * (height / 100.0)) >= 30
            THEN 1 ELSE 0 END AS is_obese
FROM patients;
```

### H3. Epilepsy patients whose doctor's first name is Lisa

All three tables; filter on columns from two of them.

```sql
SELECT p.patient_id, p.first_name, p.last_name, d.specialty
FROM patients p
JOIN admissions a ON p.patient_id = a.patient_id
JOIN doctors d    ON a.attending_doctor_id = d.doctor_id
WHERE a.diagnosis = 'Epilepsy'
  AND d.first_name = 'Lisa';
```

### H4. Temporary password for admitted patients

Password = patient\_id + length of last name + birth year. Two lessons:

- `||` produced stray ".0" characters in the video; `CONCAT` fixed the formatting.
- Joining to `admissions` repeats patients admitted twice. Use `IN (subquery)` instead of a join + `DISTINCT` to keep one row per patient.

```sql
SELECT patient_id,
       CONCAT(patient_id, LENGTH(last_name), YEAR(birth_date)) AS temp_password
FROM patients
WHERE patient_id IN (SELECT patient_id FROM admissions);
```

### H5. Total admission cost by insurance status

Even `patient_id` = insured ($10 per admission); odd = uninsured ($50). The first attempt used only `patients` and came out too low — cost is **per admission**, so count admission rows.

```sql
SELECT CASE WHEN p.patient_id % 2 = 0 THEN 'Yes' ELSE 'No' END AS has_insurance,
       SUM(CASE WHEN p.patient_id % 2 = 0 THEN 10 ELSE 50 END) AS cost_after_insurance
FROM patients p
JOIN admissions a ON p.patient_id = a.patient_id
GROUP BY has_insurance;
```

### H6. Provinces with more male than female patients

Pivot gender into two counts per province in a CTE, then compare them. Output only the province name.

```sql
WITH counts AS (
  SELECT pn.province_name,
         SUM(CASE WHEN p.gender = 'M' THEN 1 ELSE 0 END) AS male_count,
         SUM(CASE WHEN p.gender = 'F' THEN 1 ELSE 0 END) AS female_count
  FROM patients p
  JOIN province_names pn ON p.province_id = pn.province_id
  GROUP BY pn.province_name
)
SELECT province_name
FROM counts
WHERE male_count > female_count;
```

## Hard questions 7–11

These finish with multi-condition filters, percentages, `LAG`, custom sort order and a doctor × year breakdown.

### H7. Find one patient from six clues

Third letter "r" (`_` = exactly one character), female, born in Feb/May/Dec, 60–80 kg (`BETWEEN` is inclusive), odd id, from Kingston. Result: one patient.

```sql
SELECT *
FROM patients
WHERE first_name LIKE '__r%'
  AND gender = 'F'
  AND MONTH(birth_date) IN (2, 5, 12)
  AND weight BETWEEN 60 AND 80
  AND patient_id % 2 = 1
  AND city = 'Kingston';
```

### H8. Percentage of male patients, as "54.48%"

Multiply by `100.0` to avoid integer division, round to 2 decimals, then append the `%` sign (the checker compares text exactly).

```sql
SELECT CONCAT(
         ROUND(SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2),
         '%') AS percent_of_male_patients
FROM patients;
```

### H9. Admissions per day and change from the previous day

Count per date in a CTE, then `LAG(x, 1) OVER (ORDER BY date)` fetches the previous row. The first day has no previous value, so its change is NULL.

```sql
WITH daily AS (
  SELECT admission_date, COUNT(*) AS admission_day
  FROM admissions
  GROUP BY admission_date
)
SELECT admission_date,
       admission_day,
       admission_day - LAG(admission_day, 1) OVER (ORDER BY admission_date)
         AS admission_count_change
FROM daily;
```

### H10. Province names A–Z, but Ontario always first

A `CASE` in `ORDER BY` creates a sort flag: 0 for Ontario, 1 for the rest; the name breaks the tie.

```sql
SELECT province_name
FROM province_names
ORDER BY CASE WHEN province_name = 'Ontario' THEN 0 ELSE 1 END,
         province_name;
```

### H11. Admissions per doctor per year

Group by doctor id, name, specialty and admission year.

```sql
SELECT d.doctor_id,
       CONCAT(d.first_name, ' ', d.last_name) AS doctor_name,
       d.specialty,
       YEAR(a.admission_date) AS selected_year,
       COUNT(*) AS total_admissions
FROM admissions a
JOIN doctors d ON a.attending_doctor_id = d.doctor_id
GROUP BY d.doctor_id, doctor_name, d.specialty, selected_year;
```

## Patterns and common mistakes

Most of the 37 answers reuse about ten patterns.

| Pattern | Use it when | Questions |
| --- | --- | --- |
| `GROUP BY` + `HAVING COUNT(*) > 1` (or `= 1`) | Find duplicates or unique values | M2, M8, M23 |
| `COUNT(CASE WHEN … THEN id END)` / `SUM(CASE … 1 ELSE 0)` | Several conditional counts in one row (pivot) | M6, H6, H8 |
| `LIKE` with `%` and `_` | Starts/ends/contains; a letter at a fixed position | M3, M18, H7 |
| `UNION ALL` + literal column | Combine two tables into one list with a label | M10 |
| `NOT IN (subquery)` or `LEFT JOIN … IS NULL` | Rows with no match in another table | M25 |
| `IN (subquery)` instead of a join | Filter by another table without duplicating rows | H4 |
| CTE, then aggregate again | Max/min/avg of counts; compare two derived columns | M26, H6 |
| `ORDER BY … DESC LIMIT 1` | Latest / top single row | M17 |
| `FLOOR(x / 10) * 10` | Bucket numbers into ranges | H1 |
| `LAG()` window function | Compare a row with the previous row | H9 |
| `CASE` inside `ORDER BY` | Pin a value to the top, then sort the rest | H10 |

**Mistakes made (and fixed) in the video**

- Forgot `GROUP BY` — no error, just one wrong row (M11).
- Forgot the `ORDER BY` keyword and blamed the alias (M9).
- Integer division returned 0 — use `100.0` or `* 1.0` (H2, H8).
- Counted patients when the question was per admission (H5).
- Joined to `admissions` and got duplicate patients (H4).

**Clause order to remember:** `FROM` → `WHERE` → `GROUP BY` → `HAVING` → `SELECT` → `ORDER BY` → `LIMIT`. That's why a `SELECT` alias works in `ORDER BY` but, in standard SQL, not in `WHERE` or `HAVING`.

---

# Part 2 — Top 5 advanced SQL interview questions

## Overview and the granularity rule

The second video covers five advanced questions aimed at candidates with 3+ years of experience. Interviewers rarely stop at the base question — they ask follow-ups that change the grouping, so the key skill is reading the table's **granularity** (what one row represents).

- **One row per thing asked about** (e.g. one row per employee in `employee`): rank or sort directly.
- **Many rows per thing** (e.g. many order lines per product in `orders`): **aggregate first** to the asked level with `GROUP BY` + `SUM`, then rank, lag or sum.
- **"Within each …"** (department, category): add that column to the `GROUP BY` and to `PARTITION BY` in the window function.

The examples use SQL Server syntax (`TOP n`, where MySQL/PostgreSQL use `LIMIT n`) on an `employee` table and a Superstore-style `orders` table (order\_id, order\_date, product\_id, category, sales).

## Q1. Top N overall and top N within each group

Overall top N on a one-row-per-item table is just sort + `TOP`. "Within each department/category" needs a ranking window function with `PARTITION BY`.

**a) Top 2 salaries overall** — `employee` has one row per employee, so no ranking is needed. `TOP` is applied after `ORDER BY`.

```sql
SELECT TOP 2 * FROM employee ORDER BY salary DESC;   -- MySQL/Postgres: ... LIMIT 2
```

**b) Top 2 salaries in each department** — number rows inside each department, highest salary = 1, then filter in an outer query (a window alias can't be used in the same `WHERE`).

```sql
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rn
  FROM employee
) t
WHERE rn <= 2;
```

**Ties: ask the interviewer.** If two employees share a salary, should both count?

| Function | Salaries 100, 100, 90, 80 | Top-2 filter returns |
| --- | --- | --- |
| `ROW_NUMBER()` | 1, 2, 3, 4 | exactly 2 rows (tie broken arbitrarily) |
| `RANK()` | 1, 1, 3, 4 | both tied rows; skips 2 |
| `DENSE_RANK()` | 1, 1, 2, 3 | 3 rows — both tied plus the next salary |

**c) Top 5 products by sales (orders table)** — a product appears on many order rows, so `ORDER BY sales` on raw rows ranks *order lines*, not products. Aggregate to product level first.

```sql
WITH product_sales AS (
  SELECT product_id, SUM(sales) AS sales
  FROM orders
  GROUP BY product_id
)
SELECT TOP 5 * FROM product_sales ORDER BY sales DESC;
```

**d) Top 5 products within each category** — keep `category` in the aggregation, then partition by it.

```sql
WITH product_sales AS (
  SELECT category, product_id, SUM(sales) AS sales
  FROM orders
  GROUP BY category, product_id
)
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rn
  FROM product_sales
) t
WHERE rn <= 5;   -- 3 categories × 5 = 15 rows
```

## Q2. Year-over-year (YoY) growth

Aggregate to year level, fetch the previous year with `LAG`, then apply the growth formula. This tests `LAG`/`LEAD`.

YoY growth % = (current year − previous year) ÷ previous year × 100. Example: 100 → 200 → 300 gives 0%, 100%, 50%.

**a) Company-wide YoY**

- `LAG(sales, 1, sales)`: the 2nd argument is how many rows back (2 = two years back); the 3rd is the default when there is no previous row, so the first year shows 0% instead of NULL.
- No `PARTITION BY` — the whole table is one series.

```sql
WITH yearly AS (
  SELECT YEAR(order_date) AS order_year, SUM(sales) AS sales
  FROM orders
  GROUP BY YEAR(order_date)
),
with_prev AS (
  SELECT *,
         LAG(sales, 1, sales) OVER (ORDER BY order_year) AS prev_year_sales
  FROM yearly
)
SELECT *,
       (sales - prev_year_sales) * 100.0 / prev_year_sales AS yoy_growth_pct
FROM with_prev;
```

In the video's data: 2018 → 0%, 2019 → about −2%, 2020 → about +30%, 2021 → about +20%.

**b) YoY within each category** — group by category + year and add `PARTITION BY category`. Without it, the first year of "Office Supplies" would wrongly take the last "Furniture" year as its previous value.

```sql
WITH yearly AS (
  SELECT category, YEAR(order_date) AS order_year, SUM(sales) AS sales
  FROM orders
  GROUP BY category, YEAR(order_date)
),
with_prev AS (
  SELECT *,
         LAG(sales, 1, sales) OVER (PARTITION BY category ORDER BY order_year) AS prev_year_sales
  FROM yearly
)
SELECT *,
       (sales - prev_year_sales) * 100.0 / prev_year_sales AS yoy_growth_pct
FROM with_prev;
```

**c) Variation: products whose sales beat the previous month** — same shape, different grain: aggregate by product + year-month, `LAG` with `PARTITION BY product_id ORDER BY year, month`, then keep rows where `sales > prev_month_sales`.

## Q3. Cumulative (running) sum and rolling N-month sum

`SUM(…) OVER (ORDER BY …)` gives a running total; adding a `ROWS BETWEEN` frame limits it to a moving window.

| Month | Sales | Cumulative | Rolling 3 months (incl. current) |
| --- | --- | --- | --- |
| Jan | 100 | 100 | 100 |
| Feb | 200 | 300 | 300 |
| Mar | 300 | 600 | 600 |
| Apr | 400 | 1,000 | 900 (Feb–Apr) |
| May | 500 | 1,500 | 1,200 (Mar–May) |

The two match until the window fills; from April the rolling sum drops January.

**a) Cumulative sales by year** (add `category` to `GROUP BY` and `PARTITION BY category` for a per-category running total)

```sql
WITH yearly AS (
  SELECT YEAR(order_date) AS order_year, SUM(sales) AS sales
  FROM orders
  GROUP BY YEAR(order_date)          -- no ORDER BY inside a CTE/subquery in SQL Server
)
SELECT *,
       SUM(sales) OVER (ORDER BY order_year) AS cumulative_sales
FROM yearly;
```

Tie caveat: with the default frame, rows that share the same `ORDER BY` value get the same running total. That can't happen here (one row per year), but `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` avoids it when it can.

**b) Rolling 3-month sales** — aggregate to year + month, order by both, and frame the last three rows.

```sql
WITH monthly AS (
  SELECT YEAR(order_date) AS order_year, MONTH(order_date) AS order_month,
         SUM(sales) AS sales
  FROM orders
  GROUP BY YEAR(order_date), MONTH(order_date)   -- 4 years = 48 rows
)
SELECT *,
       SUM(sales) OVER (ORDER BY order_year, order_month
                        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3m_sales
FROM monthly;
```

**c) Variation: previous 3 months only** — exclude the current, possibly incomplete, month:

```sql
ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
```

For an N-month window use `N-1 PRECEDING AND CURRENT ROW` (including the current month) or `N PRECEDING AND 1 PRECEDING` (excluding it).

## Q4. Pivot rows into columns (category sales by year)

`GROUP BY year, category` gives one row per year × category. To get one row per year with a column per category, group by year only and use `SUM(CASE …)` per column.

```sql
SELECT YEAR(order_date) AS order_year,
       SUM(CASE WHEN category = 'Furniture'       THEN sales ELSE 0 END) AS furniture_sales,
       SUM(CASE WHEN category = 'Office Supplies' THEN sales ELSE 0 END) AS office_supplies_sales,
       SUM(CASE WHEN category = 'Technology'      THEN sales ELSE 0 END) AS technology_sales
FROM orders
GROUP BY YEAR(order_date)
ORDER BY order_year;
```

Follow-ups usually just change the `CASE` conditions (e.g. a region or segment); the query shape stays the same.

## Q5. Row counts for different joins

The classic puzzle: given two single-column tables with duplicates and NULLs, how many rows does each join return? The video names the example and points to a separate video for the walkthrough. Worked answer for its example:

- T1: 1, 1, 2, NULL, 3, 3
- T2: 2, NULL, 1, 1, 4, 3

| Join | Rows | Why |
| --- | --- | --- |
| INNER | 7 | 1×1 matches 2×2 = 4, 2 → 1, 3 → 2×1 = 2; NULL never equals NULL |
| LEFT | 8 | 7 + T1's unmatched NULL |
| RIGHT | 9 | 7 + T2's unmatched NULL and 4 |
| FULL OUTER | 10 | 7 + 1 unmatched left + 2 unmatched right |
| CROSS | 36 | 6 × 6 |

Rules: duplicates multiply (m × n), NULLs never match in an `=` join, and outer joins add back each unmatched row once.
