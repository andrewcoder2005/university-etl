-- OLTP and Data Warehouse schemas for the University ETL prototype

-- Create schemas (if they don't already exist)
CREATE SCHEMA IF NOT EXISTS oltp;
CREATE SCHEMA IF NOT EXISTS dw;

-- =========================
-- OLTP TABLES (schema: oltp)
-- =========================

CREATE TABLE IF NOT EXISTS oltp.students (
    student_id      INTEGER PRIMARY KEY,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    date_of_birth   DATE,
    gender          VARCHAR(20),
    country         VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oltp.courses (
    course_id       INTEGER PRIMARY KEY,
    course_code     VARCHAR(20) NOT NULL UNIQUE,
    course_name     VARCHAR(255) NOT NULL,
    department      VARCHAR(100),
    credit_points   INTEGER,
    level           VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oltp.enrolments (
    enrolment_id    INTEGER PRIMARY KEY,
    student_id      INTEGER NOT NULL REFERENCES oltp.students(student_id),
    course_id       INTEGER NOT NULL REFERENCES oltp.courses(course_id),
    enrolment_date  DATE NOT NULL,
    semester        VARCHAR(20) NOT NULL,
    status          VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS oltp.payments (
    payment_id      INTEGER PRIMARY KEY,
    student_id      INTEGER NOT NULL REFERENCES oltp.students(student_id),
    course_id       INTEGER NOT NULL REFERENCES oltp.courses(course_id),
    payment_date    DATE NOT NULL,
    amount          NUMERIC(12,2) NOT NULL,
    payment_method  VARCHAR(50)
);

-- ===================================
-- DATA WAREHOUSE TABLES (schema: dw)
-- ===================================

-- Date dimension
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key    INTEGER PRIMARY KEY,               -- e.g. 20250115
    date_actual DATE NOT NULL,
    day         INTEGER,
    month       INTEGER,
    year        INTEGER,
    month_name  VARCHAR(20),
    quarter     INTEGER
);

-- Student dimension
CREATE TABLE IF NOT EXISTS dw.dim_student (
    student_key     SERIAL PRIMARY KEY,
    student_id      INTEGER NOT NULL UNIQUE,
    full_name       VARCHAR(255),
    gender          VARCHAR(20),
    country         VARCHAR(100)
);

-- Course dimension
CREATE TABLE IF NOT EXISTS dw.dim_course (
    course_key      SERIAL PRIMARY KEY,
    course_id       INTEGER NOT NULL UNIQUE,
    course_code     VARCHAR(20),
    course_name     VARCHAR(255),
    department      VARCHAR(100),
    credit_points   INTEGER,
    level           VARCHAR(50)
);

-- Enrolment fact table
CREATE TABLE IF NOT EXISTS dw.fact_enrolments (
    fact_enrolment_id   SERIAL PRIMARY KEY,
    student_key         INTEGER NOT NULL REFERENCES dw.dim_student(student_key),
    course_key          INTEGER NOT NULL REFERENCES dw.dim_course(course_key),
    date_key            INTEGER NOT NULL REFERENCES dw.dim_date(date_key),
    enrolment_id        INTEGER NOT NULL,
    payment_amount      NUMERIC(12,2),
    enrolment_status    VARCHAR(20),
    semester            VARCHAR(20)
);

