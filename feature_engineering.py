import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor


df_ord=pd.read_csv("processed_data\order_table_t.csv")

df_op=pd.read_csv("processed_data\op_prior_t.csv")

df_ot=pd.read_csv("processed_data\op_train_t.csv")

df_p=pd.read_csv("data\products.csv")

df_o_merge= pd.concat([df_op, df_ot], axis=0)

df_ptr=df_ord.loc[df_ord['eval_set']!='test']

#Features- reorder_ratio for each user (done)
#avg. no. of product bought (d)
#user_most_freq_dept, user_most_freq_aisle (done)
#item_ratio_first_order (d)

result = pd.merge(df_o_merge, df_ptr, on='order_id', how='inner')

result = pd.merge(result, df_p, on='product_id', how='inner')

#reorder_ratio
reorder_ratio = result.groupby('user_id')['reordered'].mean().reset_index()
print("reorder_ratio-")
print(reorder_ratio.head(5))

#result = pd.merge(result, reorder_ratio, on='user_id', how='inner')

#avg_nprod
avg_nprod = result.groupby(['user_id','order_id'])['reordered'].mean().reset_index()
print("avg_nprod-")
print(avg_nprod.head(5))

#result = pd.merge(result, avg_nprod, on='user_id', how='inner')

#print(result.head())

#most_freq department
grouped = result.groupby(['user_id', 'department_id']).size().reset_index(name='order_count')

# Step 2: Identify the department with the maximum count for each user
most_frequent_dept = grouped.loc[grouped.groupby('user_id')['order_count'].idxmax()]
print("dept-")
print(most_frequent_dept.head(5))

#result = pd.merge(result, most_frequent_dept[['user_id','department_id']], on='user_id', how='inner')

#most_freq aisle
grouped = result.groupby(['user_id', 'aisle_id']).size().reset_index(name='order_count')

# Step 2: Identify the department with the maximum count for each user
most_frequent_aisle = grouped.loc[grouped.groupby('user_id')['order_count'].idxmax()]
print("aisle-")
print(most_frequent_aisle.head(5))

#result = pd.merge(result, most_frequent_aisle[['user_id','aisle_id']], on='user_id', how='inner')
#Identify total orders
total_orders = result.groupby('user_id')['order_id'].nunique().reset_index(name='total_orders')
l_d=total_orders['total_orders']

#Identify first order ratio

result_ft=result.loc[result['reordered']==0]
total_forders = result_ft.groupby('user_id')['order_id'].nunique().reset_index(name='total_orders')
l_n=total_forders['total_orders']

l_r=[]
for i in range(len(l_n)):

    n=l_n[i]
    d=l_d[i]
    l_r.append(n/d)


# Display the result
print(l_r)

#Random Forest
#regressor = RandomForestRegressor(n_estimators=10, random_state=0, oob_score=True)
 
# Fit the regressor with x and y data
#regressor.fit(X_tr, y_tr)

#y_pred=regressor.predict(X_test)
