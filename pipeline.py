# This is the main ETL pipeline script for the C1 Case Study project.
# It extracts raw POS data, transforms it into a star schema format,
# performs data cleaning, standardization and deduplication,
# and loads the cleaned data into a SQLite database for analysis.


import pandas as pd
import sqlalchemy
import unicodedata

# Create SQLAlchemy engine
engine = sqlalchemy.create_engine("sqlite:///Data/C1_case_study.db")


# Load Raw Data
# ======================
path = 'Data/POS_Data.xlsx'
raw_data = pd.read_excel(path, sheet_name='POS')

# Clean Column Names
raw_data.columns = (
    raw_data.columns
    .str.strip()
    .str.lower()
    .str.replace(' ', '_')
    .str.replace('-', '_')
    .str.replace('___', '_')
    .str.replace('__', '_')
    .str.replace('-', '_')
    .str.replace(r'[^a-z0-9_]', '', regex=True)
)
# Initial Data Cleaning
# ===========================================

# Split item_name into group and clean item name
raw_data['group'] = raw_data['item_name'].str.split(' - ', n=1).str[0].fillna(raw_data['item_name'])
raw_data['item_name'] = raw_data['item_name'].str.split(' - ', n=1).str[-1].fillna(raw_data['item_name'])

# Deduplication Process to remove day_part and category artifacts
# ================================================================

# Capture state before deduplication
rows_before = len(raw_data)
revenue_before = raw_data['gross_revenue'].sum()

# Normalize category (removes accents, makes "Entree" == "Entrée")
import unicodedata
raw_data['category_norm'] = raw_data['category'].astype(str).apply(lambda x: unicodedata.normalize('NFKD', x).encode('ascii', 'ignore').decode('utf-8'))

# Core grouping keys – exclude gross_revenue and group to avoid over-collapse
group_keys = ['check_id', 'item_name', 'date', 'sale_time_exact', 'is_beverage_on_check', 'cost_center', 'category_norm']

# Deduplicate
deduped = raw_data.groupby(group_keys, as_index=False).agg({
    'gross_revenue': 'max',
    'category': lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0],
    'day_part': lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0],
    'group': 'first'  
})

# Drop the temporary normalized column
deduped = deduped.drop(columns=['category_norm'])

# Capture state AFTER deduplication
rows_after = len(deduped)
revenue_after = deduped['gross_revenue'].sum()

rows_removed = rows_before - rows_after
rows_removed_pct = 100 * rows_removed / rows_before

revenue_correction = revenue_before - revenue_after
revenue_correction_pct = 100 * revenue_correction / revenue_before

# Print deduplication summary
print(f"Rows removed        : {rows_removed:,} ({rows_removed_pct:.2f}% of total rows)")
print(f"Revenue corrected   : ${revenue_correction:,.2f} ({revenue_correction_pct:.2f}% of original revenue)")

# Replace raw_data with the cleaned version for the rest of the pipeline
raw_data = deduped.copy()

# Split category into main and sub-category
raw_data['category_main'] = raw_data['category'].str.split('>', n=1).str[0].str.strip()
raw_data['sub_category'] = raw_data['category'].str.split('>', n=1).str[-1].str.strip()

# Combine date + time into proper timestamp
raw_data['timestamp'] = pd.to_datetime(
    raw_data['date'].astype(str) + ' ' + raw_data['sale_time_exact'].astype(str),
    errors='coerce'
)

# Convert is_beverage_on_check to boolean
raw_data['is_beverage_on_check'] = raw_data['is_beverage_on_check'].str.lower().str.strip() == 'yes'


# Build Dimension Tables First (with surrogate keys)
# =========================================================

# # ---- categories table (dim_categories) ----
# categories = (
#     raw_data[['category_main', 'sub_category', 'cost_center']]
#     .drop_duplicates()
#     .sort_values(['category_main', 'sub_category'])
#     .reset_index(drop=True)
# )
# categories['category_id'] = categories.index + 1


# Read pre-defined dim_categories with margin groups
dim_categories = pd.read_excel("Data/dim_categories.xlsx", sheet_name='dim_categories')
# Derive margin
dim_categories['margin'] = dim_categories['margin_group'].map({'Beverage': 0.6, 'Food': 0.4, 'Snacks': 0.3})



# ---- items table (dim_items) ----
items_raw = raw_data[['item_name', 'group', 'category_main', 'sub_category', 'gross_revenue', 'cost_center']].copy()

# Find the most common non-zero gross_revenue per item for the unit price
mode_price = (
    items_raw[items_raw['gross_revenue'] > 0]
    .groupby(['item_name', 'group', 'category_main', 'sub_category', 'cost_center'])
    ['gross_revenue']
    .agg(lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0])
    .reset_index(name='unit_price')
)

# Merge back and create final dim_items
items = mode_price.copy()
items = items.rename(columns={'unit_price': 'price'})

items['item_id'] = range(1, len(items) + 1)
items = items[['item_id', 'item_name', 'group', 'category_main', 'sub_category', 'price', 'cost_center']]


# ---- transactions table (fact_transactions) ----
transactions = (
    raw_data.groupby('check_id', as_index=False).agg(
        timestamp=('timestamp', 'first'),
        total_amount=('gross_revenue', 'sum'),
        num_items=('item_name', 'count'),
        cost_center=('cost_center', 'first'),
        day_part=('day_part', 'first'),
        top_group=('group', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        is_beverage_on_check=('is_beverage_on_check', 'first')
    )
)
transactions['transaction_id'] = transactions.index + 1
transactions = transactions[['transaction_id', 'check_id', 'timestamp', 'total_amount', 'num_items', 'cost_center', 'day_part', 'top_group', 'is_beverage_on_check']]


# Build Fact Table: line_items (with FKs)
# ===========================================

# Start from raw data
line_items = raw_data[['check_id', 'item_name', 'gross_revenue', 'timestamp', 
                       'day_part', 'is_beverage_on_check', 'group', 'category_main', 
                       'sub_category', 'cost_center']].copy()

# Merge with items on ALL unique keys
line_items = line_items.merge(
    items[['item_id', 'item_name', 'group', 'category_main', 'sub_category', 'cost_center']],
    on=['item_name', 'group', 'category_main', 'sub_category', 'cost_center'],
    how='left'
)

# Merge with transactions to get transaction_id
line_items = line_items.merge(
    transactions[['transaction_id', 'check_id']],
    on='check_id',
    how='left'
)

# Final line_items fact table
line_items_final = line_items[['transaction_id', 'item_id', 'gross_revenue', 'timestamp', 'day_part', 'is_beverage_on_check']].copy()

line_items_final['line_item_id'] = range(1, len(line_items_final) + 1)
line_items_final = line_items_final[['line_item_id', 'transaction_id', 'item_id', 'gross_revenue', 'timestamp', 'day_part', 'is_beverage_on_check']]


# FINAL CLEAN-UP BEFORE WRITING TO DATABASE 
# =============================================

# 1. dim_items — keep only what belongs
dim_items = items[['item_id', 'item_name', 'group', 'category_main', 'sub_category', 'price', 'cost_center']]
dim_items = dim_items.rename(columns={
    'category_main': 'category',
    'cost_center': 'item_cost_center'   # ← rename to avoid conflict
})

# Join Margin data and category_id from category table
dim_items = dim_items.merge(
    dim_categories[['cat_level1', 'cat_level2', 'cat_cost_center', 'margin', 'category_id']],
    left_on=['category', 'sub_category', 'item_cost_center'],  # Adjust column names as needed
    right_on=['cat_level1', 'cat_level2', 'cat_cost_center'],
    how='left'
)

# Calculate est_cost in dim_items (item-level)
dim_items['est_cost'] = dim_items['price'] * (1 - dim_items['margin'])

# Fill any missing values 0.0
dim_items = dim_items.fillna(0)

# Enforce data types
dim_items = dim_items.astype({
    'item_id': 'int32',
    'item_name': 'string',
    'group': 'string',
    'category': 'string',
    'sub_category': 'string',
    'price': 'float32',
    'item_cost_center': 'string',
    'margin': 'float32',
    'est_cost': 'float32',
    'category_id': 'int32'
})

# # 2. dim_categories 
# *Commented in favor of static categories table loaded above
# dim_categories = categories[['category_id', 'category_main', 'sub_category', 'cost_center']].copy()
# dim_categories = dim_categories.rename(columns={
#     'category_main': 'cat_level1',
#     'sub_category': 'cat_level2',
#     'cost_center': 'cat_cost_center'   # ← avoid conflict
# })

# Enforce data types
dim_categories = dim_categories.astype({
    'category_id': 'int32',
    'cat_level1': 'string',
    'cat_level2': 'string',
    'cat_cost_center': 'string',
    'margin_group': 'string',
    'margin': 'float32'
})

# 3. fact_transactions — remove fields that also exist in line_items
fact_transactions = transactions[[
    'transaction_id', 'check_id', 'timestamp', 'total_amount', 
    'num_items', 'cost_center', 'top_group', 'is_beverage_on_check', 'day_part'
]].copy()

# Rename to be explicit
fact_transactions = fact_transactions.rename(columns={
    'cost_center': 'transaction_cost_center',
    'is_beverage_on_check': 'has_beverage'
})

# Enforce data types
fact_transactions = fact_transactions.astype({
    'transaction_id': 'int32',
    'check_id': 'int32',
    'timestamp': 'datetime64[ns]',
    'total_amount': 'float32',
    #Removed in favor of line_items quantity 'num_items': 'int32',
    'transaction_cost_center': 'string',
    'top_group': 'string',
    'has_beverage': 'bool',
    'day_part': 'string'
})

# 4. fact_line_items — remove ALL fields that exist elsewhere
fact_line_items = line_items_final[[
    'line_item_id', 'transaction_id', 'item_id', 'gross_revenue'
]].copy()

# Merge in margin and price from dim_items for calculations
fact_line_items = fact_line_items.merge(
    dim_items[['item_id', 'margin', 'price']],
    on='item_id',
    how='left'
)

fact_line_items['est_cost'] = fact_line_items['gross_revenue'] * (1 - fact_line_items['margin'])
fact_line_items['est_profit'] = fact_line_items['gross_revenue'] - fact_line_items['est_cost']

# Add quantity based on multiples of price
fact_line_items['quantity'] = fact_line_items['gross_revenue'] / fact_line_items['price']
fact_line_items['quantity'] = fact_line_items['quantity'].fillna(0).round().astype('int32')

# Test quantity consistency
non_zero = fact_line_items[fact_line_items['gross_revenue'] > 0]
integer_qty = non_zero['quantity'].apply(lambda x: x == int(x)).sum()
integer_pct = (integer_qty / len(non_zero)) * 100 if len(non_zero) > 0 else 0

print(f"Quantity consistency: {integer_qty:,} integer quantities out of {len(non_zero):,} non-zero rows ({integer_pct:.2f}%)")

# Fill NAs with 0 before type enforcement
fact_line_items = fact_line_items.fillna({
    'line_item_id': 0,
    'transaction_id': 0,
    'item_id': 0,
    'gross_revenue': 0,
    'margin': 0,
    'est_cost': 0,
    'est_profit': 0,
    'quantity': 1
})

# Enforce data tpyes
fact_line_items = fact_line_items.astype({
    'line_item_id': 'int32',
    'transaction_id': 'int32',
    'item_id': 'int32',
    'gross_revenue': 'float32',
    'margin': 'float32',
    'est_cost': 'float32',
    'est_profit': 'float32'
})

# Load cleaned tables to the database
# ==============================================

dim_items.to_sql('dim_items', engine, if_exists='replace', index=False)
dim_categories.to_sql('dim_categories', engine, if_exists='replace', index=False)
fact_transactions.to_sql('fact_transactions', engine, if_exists='replace', index=False)
fact_line_items.to_sql('fact_line_items', engine, if_exists='replace', index=False)


print("Pipeline executed successfully. Summary of loaded tables and rows:")
print(f"- dim_items > {len(dim_items)} items")
print(f"- dim_categories > {len(dim_categories)} categories")
print(f"- fact_transactions > {len(fact_transactions)} transactions")
print(f"- fact_line_items > {len(fact_line_items)} line items")