# pipeline.py - Fully Relational Version with Proper Foreign Keys
import pandas as pd
import sqlalchemy

# Create SQLAlchemy engine
engine = sqlalchemy.create_engine("sqlite:///C1_case_study.db")


# 1. Load Raw Data
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


# 2. Feature Engineering
# ===========================================
raw_data['group'] = raw_data['item_name'].str.split(' - ', n=1).str[0].fillna(raw_data['item_name'])
raw_data['item_name_clean'] = raw_data['item_name'].str.split(' - ', n=1).str[-1].fillna(raw_data['item_name'])
raw_data['category_main'] = raw_data['category'].str.split('>', n=1).str[0].str.strip()
raw_data['sub_category'] = raw_data['category'].str.split('>', n=1).str[-1].str.strip()

# Combine date + time into proper timestamp
raw_data['timestamp'] = pd.to_datetime(
    raw_data['date'].astype(str) + ' ' + raw_data['sale_time_exact'].astype(str),
    errors='coerce'
)

raw_data['is_beverage_on_check'] = raw_data['is_beverage_on_check'].str.lower().str.strip() == 'yes'


# 3. Build Dimension Tables First (with surrogate keys)
# =========================================================

# ---- items table (dim_items) ----
items_raw = raw_data[['item_name_clean', 'group', 'category_main', 'sub_category', 'gross_revenue', 'cost_center']].copy()
items_raw = items_raw.rename(columns={'item_name_clean': 'item_name', 'gross_revenue': 'price'})

# Use latest price per item (or average if you prefer)
items = (
    items_raw
    .groupby(['item_name', 'group', 'category_main', 'sub_category', 'cost_center'], as_index=False)
    .agg(price=('price', 'max'))  
    .sort_values('item_name')
    .reset_index(drop=True)
)

items['item_id'] = items.index + 1  
items = items[['item_id', 'item_name', 'group', 'category_main', 'sub_category', 'price', 'cost_center']]

# ---- categories table (dim_categories) ----
categories = (
    raw_data[['category_main', 'sub_category', 'cost_center']]
    .drop_duplicates()
    .sort_values(['category_main', 'sub_category'])
    .reset_index(drop=True)
)
categories['category_id'] = categories.index + 1

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


# 4. Build Fact Table: line_items (with FKs)
# ===========================================

# Start from raw data
line_items = raw_data[['check_id', 'item_name_clean', 'gross_revenue', 'timestamp', 'day_part', 'is_beverage_on_check']].copy()

line_items = line_items.rename(columns={'item_name_clean': 'item_name'})

# Merge with items to get item_id
line_items = line_items.merge(
    items[['item_id', 'item_name']],
    on='item_name',
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


# 5. Write All Tables to SQLite with Proper Order
# ==================================================

# Write dimension tables first
items.to_sql('dim_items', engine, if_exists='replace', index=False)
categories.to_sql('dim_categories', engine, if_exists='replace', index=False)

# Then fact tables
transactions.to_sql('fact_transactions', engine, if_exists='replace', index=False)
line_items_final.to_sql('fact_line_items', engine, if_exists='replace', index=False)

print("Relational database built successfully!")
print(f"   • {len(items)} items")
print(f"   • {len(categories)} categories")
print(f"   • {len(transactions)} transactions")
print(f"   • {len(line_items_final)} line items")

# # Add indexes for performance
# with engine.connect() as conn:
#     conn.execute(sqlalchemy.text("CREATE INDEX IF NOT EXISTS idx_line_transaction ON fact_line_items(transaction_id);"))
#     conn.execute(sqlalchemy.text("CREATE INDEX IF NOT EXISTS idx_line_item ON fact_line_items(item_id);"))
#     conn.execute(sqlalchemy.text("CREATE INDEX IF NOT EXISTS idx_trans_check ON fact_transactions(check_id);"))


