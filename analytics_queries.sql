-- Analytical SQL queries for the University Data Warehouse
-- Assumes ETL has populated dw.dim_* and dw.fact_enrolments

-- 1. Total enrolments per semester
SELECT
    semester,
    COUNT(*) AS total_enrolments
FROM dw.fact_enrolments
GROUP BY semester
ORDER BY semester;


-- 2. Revenue per course
SELECT
    dc.course_code,
    dc.course_name,
    SUM(fe.payment_amount) AS total_revenue
FROM dw.fact_enrolments fe
JOIN dw.dim_course dc
  ON fe.course_key = dc.course_key
GROUP BY dc.course_code, dc.course_name
ORDER BY total_revenue DESC;


-- 3. Average student load (average number of courses per student per semester)
SELECT
    semester,
    AVG(course_count) AS avg_courses_per_student
FROM (
    SELECT
        semester,
        student_key,
        COUNT(*) AS course_count
    FROM dw.fact_enrolments
    GROUP BY semester, student_key
) sub
GROUP BY semester
ORDER BY semester;


-- 4. Monthly revenue trends
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    SUM(fe.payment_amount) AS total_revenue
FROM dw.fact_enrolments fe
JOIN dw.dim_date dd
  ON fe.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;


-- 5. Top 5 courses by revenue (overall)
SELECT
    dc.course_code,
    dc.course_name,
    SUM(fe.payment_amount) AS total_revenue,
    RANK() OVER (ORDER BY SUM(fe.payment_amount) DESC) AS revenue_rank
FROM dw.fact_enrolments fe
JOIN dw.dim_course dc
  ON fe.course_key = dc.course_key
GROUP BY dc.course_code, dc.course_name
ORDER BY total_revenue DESC
LIMIT 5;


-- 6. Window function example: revenue rank per semester
SELECT
    fe.semester,
    dc.course_code,
    dc.course_name,
    SUM(fe.payment_amount) AS total_revenue,
    RANK() OVER (
        PARTITION BY fe.semester
        ORDER BY SUM(fe.payment_amount) DESC
    ) AS revenue_rank_in_semester
FROM dw.fact_enrolments fe
JOIN dw.dim_course dc
  ON fe.course_key = dc.course_key
GROUP BY fe.semester, dc.course_code, dc.course_name
ORDER BY fe.semester, revenue_rank_in_semester;

