CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY,
    name TEXT,
    department TEXT,
    salary INTEGER
);

INSERT INTO
    employees (name, department, salary)
VALUES ('John', 'IT', 50000),
    ('Mary', 'HR', 45000),
    ('David', 'Marketing', 48000);

SELECT * FROM employees;

SELECT name, salary FROM employees WHERE salary > 46000;

SELECT department, salary FROM employees;

SELECT department, AVG(salary) as avg_salary
FROM employees
GROUP BY
    department;