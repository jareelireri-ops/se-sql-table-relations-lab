import sqlite3
import pandas as pd

conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
df_boston = pd.read_sql("""
SELECT e.firstName, e.lastName
FROM employees e
JOIN offices o ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
""", conn)

# STEP 2
df_zero_emp = pd.read_sql("""
SELECT o.city, COUNT(e.employeeNumber) AS num_employees
FROM offices o
JOIN employees e ON o.officeCode = e.officeCode
GROUP BY o.city
HAVING COUNT(e.employeeNumber) = 0
""", conn)

# STEP 3
df_employee = pd.read_sql("""
SELECT e.firstName, e.lastName, e.employeeNumber, c.customerNumber
FROM employees e
LEFT JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
ORDER BY e.firstName
""", conn)

# STEP 4 - FIX: remove DISTINCT, use simple LEFT JOIN with IS NULL
df_contacts = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.customerNumber, c.customerName
FROM customers c
LEFT JOIN orders o ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL
""", conn)

# STEP 5
df_payment = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.customerNumber, p.amount
FROM customers c
JOIN payments p ON c.customerNumber = p.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC
""", conn)

# STEP 6
df_credit = pd.read_sql("""
SELECT e.firstName, e.lastName, e.employeeNumber,
       COUNT(c.customerNumber) AS num_customers
FROM employees e
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber
HAVING AVG(c.creditLimit) > 90000
ORDER BY num_customers DESC
""", conn)

# STEP 7
df_product_sold = pd.read_sql("""
SELECT p.productCode, p.productName,
       SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
GROUP BY p.productCode
ORDER BY totalunits DESC
""", conn)

# STEP 8 - FIX: remove LIMIT 12
df_total_customers = pd.read_sql("""
SELECT p.productCode, p.productName,
       COUNT(DISTINCT c.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
JOIN orders o ON od.orderNumber = o.orderNumber
JOIN customers c ON o.customerNumber = c.customerNumber
GROUP BY p.productCode, p.productName
ORDER BY numpurchasers DESC
""", conn)

# STEP 9
df_customers = pd.read_sql("""
SELECT e.employeeNumber, e.firstName, e.lastName,
       COUNT(c.customerNumber) AS n_customers
FROM employees e
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber
ORDER BY n_customers DESC
""", conn)

# STEP 10 - FIX: remove LIMIT, fix HAVING threshold
df_under_20 = pd.read_sql("""
SELECT e.firstName, e.lastName, c.customerName, p.productName, p.productCode
FROM employees e
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders o ON c.customerNumber = o.customerNumber
JOIN orderdetails od ON o.orderNumber = od.orderNumber
JOIN products p ON od.productCode = p.productCode
WHERE p.productCode IN (
    SELECT productCode
    FROM orderdetails
    GROUP BY productCode
    HAVING SUM(quantityOrdered) < 20
)
ORDER BY e.firstName
""", conn)

conn.close()