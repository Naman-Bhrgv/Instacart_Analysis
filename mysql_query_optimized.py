import pandas as pd
import sqlite3
import time

start_time = time.time()

aisle_df=pd.read_csv("data\\aisles.csv")

dept_df=pd.read_csv("data\departments.csv")

op_prior_df=pd.read_csv("data\order_products__prior.csv")

op_train_df=pd.read_csv("data\order_products__train.csv")

ord_df=pd.read_csv("data\orders.csv")

prod_df=pd.read_csv("data\products.csv")

connection = sqlite3.connect(':memory:')

aisle_df.to_sql('aisles', connection, index=False, if_exists='replace')
dept_df.to_sql('dept', connection, index=False, if_exists='replace')
op_prior_df.to_sql('op_prior', connection, index=False, if_exists='replace')
op_train_df.to_sql('op_train', connection, index=False, if_exists='replace')
ord_df.to_sql('order_table', connection, index=False, if_exists='replace')
prod_df.to_sql('product', connection, index=False, if_exists='replace')

# Create a cursor object
cursor = connection.cursor()

q0="CREATE INDEX idx_dept ON dept (department)";
q0_1="CREATE INDEX idx_aisle ON aisles (aisle)";
q0_2="CREATE INDEX idx_pn ON product (product_name)";
q0_3="CREATE INDEX idx_pid ON op_prior (product_id)";
q0_4="CREATE INDEX idx_pid ON op_train (product_id)";

# Run SQL queries
#Remove duplicate
q1 = 'SELECT DISTINCT * FROM order_table;'
q2='SELECT DISTINCT * FROM op_train;'
q3='SELECT DISTINCT * FROM op_prior;'
q4='SELECT DISTINCT * FROM dept;'
q5='SELECT DISTINCT * FROM aisles;'
q6='SELECT DISTINCT * FROM product;'


#Trim string columns
q7='UPDATE order_table SET eval_set = TRIM(eval_set);'
q8='UPDATE dept SET department = TRIM(department);'
q9='UPDATE product SET product_name = TRIM(product_name);'
q10='UPDATE aisles SET aisle = TRIM(aisle);'
q11='UPDATE product SET aisle_id = TRIM(aisle_id);'
q12='UPDATE product SET department_id = TRIM(department_id);'

#Lowercase string
q13='UPDATE dept SET department = LOWER(department);'
q14='UPDATE aisles SET aisle = LOWER(aisle);'
q15='UPDATE product SET product_name = LOWER(product_name);'
q16='UPDATE aisles SET aisle = LOWER(aisle);'
q17='UPDATE product SET aisle_id = LOWER(aisle_id);'
q18='UPDATE product SET department_id = LOWER(department_id);'
#verify data integrity
#q13='ALTER TABLE order_table ADD CONSTRAINT pk_constraint PRIMARY KEY (order_id);'
#q14='ALTER TABLE dept ADD CONSTRAINT pk_constraint PRIMARY KEY (department_id);'
#q15='ALTER TABLE product ADD CONSTRAINT pk_constraint PRIMARY KEY (product_id);'

#delete rows with null
q19='DELETE FROM dept WHERE department_id IS NULL OR department IS NULL';
q20='DELETE FROM aisles WHERE aisle_id IS NULL OR aisle IS NULL';
q21='DELETE FROM order_table WHERE order_id IS NULL OR user_id IS NULL OR eval_set IS NULL OR order_number IS NULL OR order_dow IS NULL OR days_since_prior_order IS NULL';
q22='DELETE FROM product WHERE product_id IS NULL OR product_name IS NULL OR aisle_id IS NULL OR department_id IS NULL';
q23='DELETE FROM op_prior WHERE order_id IS NULL OR product_id IS NULL OR add_to_cart_order IS NULL OR reordered IS NULL';
q24='DELETE FROM op_train WHERE order_id IS NULL OR product_id IS NULL OR add_to_cart_order IS NULL OR reordered IS NULL';

cursor.execute(q0)
cursor.execute(q0_1)
cursor.execute(q0_2)
cursor.execute(q1)
cursor.execute(q2)
cursor.execute(q3)
cursor.execute(q4)
cursor.execute(q5)
cursor.execute(q6)
cursor.execute(q7)
cursor.execute(q8)
cursor.execute(q9)
cursor.execute(q10)
cursor.execute(q11)
cursor.execute(q12)
cursor.execute(q13)
cursor.execute(q14)
cursor.execute(q15)
cursor.execute(q16)
cursor.execute(q17)
cursor.execute(q18)
cursor.execute(q19)
cursor.execute(q20)
cursor.execute(q21)
cursor.execute(q22)
cursor.execute(q23)
cursor.execute(q24)

end_time = time.time()


connection.close()

print(end_time-start_time)
