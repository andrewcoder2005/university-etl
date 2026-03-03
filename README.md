## University Data ETL & Warehouse

This is a small practice project where I simulate a university’s data pipeline and build a simple data warehouse on top of PostgreSQL.  
The goal is to show that I can work with CSV data, design schemas, write ETL code, and write analytical SQL queries.

---

## 1. What this project does

The project works with a few fake CSV files that represent university data:

- **students** – who is enrolled
- **courses** – what courses exist
- **enrolments** – which student took which course and when
- **payments** – how much was paid, when, and by whom

From there, it:

1. **Loads the CSV files** into an OLTP-style schema in Postgres (`oltp` schema).
2. **Builds a simple star-schema warehouse** in a separate `dw` schema.
3. **Populates dimension tables** (students, courses, dates).
4. **Builds a fact table** for enrolments and revenue.
5. **Runs analytical SQL queries** to answer questions about enrolments and revenue.

---

## 2. Tech stack

- **Language**: Python 3.10+
- **Database**: PostgreSQL (local or remote)
- **Libraries**:
  - `pandas`
  - `psycopg2-binary`
  - `python-dotenv`

---

## 3. Data model (short version)

### 3.1 OLTP schema (`oltp`)

- `oltp.students`
- `oltp.courses`
- `oltp.enrolments`
- `oltp.payments`

These tables are close to how you might store the data in a transactional system.

### 3.2 Data warehouse schema (`dw`)

- **Fact table**
  - `dw.fact_enrolments` – one row per enrolment, with revenue and status.
- **Dimension tables**
  - `dw.dim_student`
  - `dw.dim_course`
  - `dw.dim_date`

This is a basic star schema that makes it easier to query things like:
- total revenue over time,
- enrolments per semester,
- most popular or highest-revenue courses.

---

## 4. Project structure

university-etl/
  README.md
  requirements.txt
  schema.sql
  etl.py
  analytics_queries.sql
  .env.example
  data/
    raw/
      students.csv
      courses.csv
      enrolments.csv
      payments.csv
