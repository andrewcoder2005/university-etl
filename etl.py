import os
from pathlib import Path
from typing import Tuple

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import execute_values
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "raw"
SCHEMA_SQL_PATH = BASE_DIR / "schema.sql"


def load_env() -> None:
    """Load environment variables from .env if present."""
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def get_db_connection() -> PGConnection:
    """Create a PostgreSQL connection using environment variables."""
    load_env()
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "university_etl"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )
    conn.autocommit = False
    return conn


def initialise_schema(conn: PGConnection) -> None:
    """Execute schema.sql to create schemas and tables if they do not exist."""
    print("Initialising database schema...")
    with conn.cursor() as cur, SCHEMA_SQL_PATH.open("r", encoding="utf-8") as f:
        sql = f.read()
        cur.execute(sql)
    conn.commit()
    print("Schema initialised.")


def load_csvs() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load raw CSV files into pandas DataFrames and perform basic cleaning."""
    print("Loading CSV files from data/raw ...")
    students = pd.read_csv(DATA_DIR / "students.csv")
    courses = pd.read_csv(DATA_DIR / "courses.csv")
    enrolments = pd.read_csv(DATA_DIR / "enrolments.csv")
    payments = pd.read_csv(DATA_DIR / "payments.csv")

    # Basic cleaning / type conversions
    date_cols = ["date_of_birth"]
    for col in date_cols:
        if col in students.columns:
            students[col] = pd.to_datetime(students[col], errors="coerce").dt.date

    enrolments["enrolment_date"] = pd.to_datetime(
        enrolments["enrolment_date"], errors="coerce"
    ).dt.date
    payments["payment_date"] = pd.to_datetime(
        payments["payment_date"], errors="coerce"
    ).dt.date

    print("CSV files loaded and cleaned.")
    return students, courses, enrolments, payments


def truncate_oltp_tables(conn: PGConnection) -> None:
    """Truncate OLTP tables before reloading data."""
    print("Truncating OLTP tables...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE oltp.payments, oltp.enrolments, oltp.courses, oltp.students RESTART IDENTITY CASCADE;")
    conn.commit()
    print("OLTP tables truncated.")


def bulk_insert_dataframe(
    conn: PGConnection,
    df: pd.DataFrame,
    table: str,
    columns: Tuple[str, ...],
) -> None:
    """Bulk insert a DataFrame into a Postgres table using execute_values."""
    print(f"Inserting into {table} ({len(df)} rows)...")
    # Build a list of tuples in the same column order expected by the table.
    # Use a different variable name for each row to avoid shadowing `df`.
    # Convert `columns` to a list so pandas treats it as a list of column labels.
    records = [
        tuple(row[col] for col in columns)
        for _, row in df[list(columns)].iterrows()
    ]
    with conn.cursor() as cur:
        template = "(" + ",".join(["%s"] * len(columns)) + ")"
        execute_values(
            cur,
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s",
            records,
            template=template,
        )
    conn.commit()
    print(f"Inserted {len(records)} rows into {table}.")


def load_oltp(conn: PGConnection,
              students: pd.DataFrame,
              courses: pd.DataFrame,
              enrolments: pd.DataFrame,
              payments: pd.DataFrame) -> None:
    """Load cleaned DataFrames into OLTP tables."""
    truncate_oltp_tables(conn)

    bulk_insert_dataframe(
        conn,
        students,
        "oltp.students",
        ("student_id", "first_name", "last_name", "date_of_birth", "gender", "country"),
    )
    bulk_insert_dataframe(
        conn,
        courses,
        "oltp.courses",
        ("course_id", "course_code", "course_name", "department", "credit_points", "level"),
    )
    bulk_insert_dataframe(
        conn,
        enrolments,
        "oltp.enrolments",
        ("enrolment_id", "student_id", "course_id", "enrolment_date", "semester", "status"),
    )
    bulk_insert_dataframe(
        conn,
        payments,
        "oltp.payments",
        ("payment_id", "student_id", "course_id", "payment_date", "amount", "payment_method"),
    )


def populate_dim_date(conn: PGConnection) -> None:
    """
    Populate dw.dim_date based on distinct dates from enrolments and payments.
    This is a simple date dimension just for the prototype.
    """
    print("Populating dw.dim_date ...")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw.dim_date (date_key, date_actual, day, month, year, month_name, quarter)
            SELECT DISTINCT
                EXTRACT(YEAR FROM d)::INT * 10000
                + EXTRACT(MONTH FROM d)::INT * 100
                + EXTRACT(DAY FROM d)::INT         AS date_key,
                d                                   AS date_actual,
                EXTRACT(DAY FROM d)::INT           AS day,
                EXTRACT(MONTH FROM d)::INT         AS month,
                EXTRACT(YEAR FROM d)::INT          AS year,
                TO_CHAR(d, 'Mon')                  AS month_name,
                EXTRACT(QUARTER FROM d)::INT       AS quarter
            FROM (
                SELECT enrolment_date AS d FROM oltp.enrolments
                UNION
                SELECT payment_date AS d FROM oltp.payments
            ) dates
            ON CONFLICT (date_key) DO NOTHING;
            """
        )
    conn.commit()
    print("dw.dim_date populated.")


def populate_dim_student(conn: PGConnection) -> None:
    """Populate dw.dim_student from oltp.students."""
    print("Populating dw.dim_student ...")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw.dim_student (student_id, full_name, gender, country)
            SELECT
                s.student_id,
                s.first_name || ' ' || s.last_name AS full_name,
                s.gender,
                s.country
            FROM oltp.students s
            ON CONFLICT (student_id) DO NOTHING;
            """
        )
    conn.commit()
    print("dw.dim_student populated.")


def populate_dim_course(conn: PGConnection) -> None:
    """Populate dw.dim_course from oltp.courses."""
    print("Populating dw.dim_course ...")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw.dim_course (course_id, course_code, course_name, department, credit_points, level)
            SELECT
                c.course_id,
                c.course_code,
                c.course_name,
                c.department,
                c.credit_points,
                c.level
            FROM oltp.courses c
            ON CONFLICT (course_id) DO NOTHING;
            """
        )
    conn.commit()
    print("dw.dim_course populated.")


def truncate_dw_facts(conn: PGConnection) -> None:
    """Truncate fact table while keeping dimensions."""
    print("Truncating dw.fact_enrolments ...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dw.fact_enrolments RESTART IDENTITY CASCADE;")
    conn.commit()
    print("dw.fact_enrolments truncated.")


def populate_fact_enrolments(conn: PGConnection) -> None:
    """
    Populate dw.fact_enrolments by joining OLTP tables and mapping to dimension keys.
    Assumes dimensions are already populated.
    """
    print("Populating dw.fact_enrolments ...")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw.fact_enrolments (
                student_key,
                course_key,
                date_key,
                enrolment_id,
                payment_amount,
                enrolment_status,
                semester
            )
            SELECT
                ds.student_key,
                dc.course_key,
                dd.date_key,
                e.enrolment_id,
                COALESCE(p.amount, 0) AS payment_amount,
                e.status              AS enrolment_status,
                e.semester
            FROM oltp.enrolments e
            JOIN dw.dim_student ds
              ON ds.student_id = e.student_id
            JOIN dw.dim_course dc
              ON dc.course_id = e.course_id
            JOIN dw.dim_date dd
              ON dd.date_actual = e.enrolment_date
            LEFT JOIN oltp.payments p
              ON p.student_id = e.student_id
             AND p.course_id = e.course_id
             AND p.payment_date >= e.enrolment_date
            ;
            """
        )
    conn.commit()
    print("dw.fact_enrolments populated.")


def run_etl() -> None:
    """Main ETL orchestration function."""
    conn = get_db_connection()
    try:
        initialise_schema(conn)
        students, courses, enrolments, payments = load_csvs()
        load_oltp(conn, students, courses, enrolments, payments)

        populate_dim_date(conn)
        populate_dim_student(conn)
        populate_dim_course(conn)

        truncate_dw_facts(conn)
        populate_fact_enrolments(conn)

        conn.commit()
        print("ETL completed successfully.")
    except Exception as exc:
        conn.rollback()
        print(f"ETL failed: {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_etl()

