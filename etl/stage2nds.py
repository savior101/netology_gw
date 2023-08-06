import pandas as pd
from os import path
from sqlalchemy import create_engine, text
from datetime import date, timedelta, datetime


def stage2nds_task(hostname
                , login
                , password
                , db
                , source_schema
                , source_table
                , target_schema
                , target_tables
                , today
                , is_manual):
    
    def db_connect(hostname, login, password, db):
        engine = create_engine(f'postgresql://{login}:{password}@{hostname}/{db}')
        con = engine.connect()
        return con
    
    def db_disconnect(con):
        con.close()
    
    def stage_1(stage_df, target_schema, target_tables):
        con = db_connect(hostname, login, password, db)

        query = text(f"select {', '.join(target_tables['dict_gross_margin_perc'])} from {target_schema}.dict_gross_margin_perc")
        exists_gross_margin_df = pd.read_sql(sql=query, con=con)

        gross_margin_df = stage_df[['gross_margin_perc']].copy()
        gross_margin_df.drop_duplicates(inplace=True)

        new_gross_margin = gross_margin_df.merge(exists_gross_margin_df, how='left', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1)[gross_margin_df.columns]
        new_gross_margin.to_sql(name='dict_gross_margin_perc', con=con, schema=target_schema, if_exists='append', index=False)
        
        # con.commit()
        db_disconnect(con)
    
    def stage_2(stage_df, target_schema, target_tables):
        invoices_df = stage_df[['invoice_id'
                        , 'unit_price'
                        , 'quantity'
                        , 'tax_5perc'
                        , 'total'
                        , 'date'
                        , 'time'
                        , 'cogs'
                        , 'gross_margin_perc'
                        , 'gross_income'
                        , 'rating']].copy()
        invoices_df.rename(columns={'tax_5perc': 'tax'}, inplace=True)

        con = db_connect(hostname, login, password, db)

        query = text(f"select gross_margin_perc_id, {', '.join(target_tables['dict_gross_margin_perc'])} from {target_schema}.dict_gross_margin_perc")
        exists_gross_margin_df = pd.read_sql(sql=query, con=con)

        invoices_df = invoices_df.merge(exists_gross_margin_df, on='gross_margin_perc')
        invoices_df = invoices_df.loc[:, invoices_df.columns!='gross_margin_perc']

        invoices_df.to_sql(name='invoices', con=con, schema=target_schema, if_exists='append', index=False)
        
        # con.commit()
        db_disconnect(con)
    
    def stage_3(stage_df, target_schema, target_tables):
        con = db_connect(hostname, login, password, db)
        
        # branches
        query = text(f"select {', '.join(target_tables['branches'])} from {target_schema}.branches")
        exists_branches_df = pd.read_sql(sql=query, con=con)

        branches_df = stage_df[['branch', 'city']].copy()
        branches_df.rename(columns={'branch': 'branch_name'}, inplace=True)
        branches_df.drop_duplicates(inplace=True)

        new_branches = branches_df.merge(exists_branches_df, how='left', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1)[branches_df.columns]
        new_branches['start_ts'] = date.today()
        new_branches['end_ts'] = datetime.strptime('01/01/5999', "%m/%d/%Y")
        new_branches['is_current'] = True
        new_branches['version'] = 1

        new_branches.to_sql(name='branches', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        # customers
        query = text(f"select {', '.join(target_tables['customers'])} from {target_schema}.customers")
        exists_customers_df = pd.read_sql(sql=query, con=con)

        customers_df = stage_df[['customer_type', 'gender']].copy()
        customers_df.drop_duplicates(inplace=True)

        new_customers = customers_df.merge(exists_customers_df, how='left', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1)[customers_df.columns]
        new_customers['start_ts'] = date.today()
        new_customers['end_ts'] = datetime.strptime('01/01/5999', "%m/%d/%Y")
        new_customers['is_current'] = True
        new_customers['version'] = 1

        new_customers.to_sql(name='customers', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        # payments
        query = text(f"select {', '.join(target_tables['payments'])} from {target_schema}.payments")
        exists_payments_df = pd.read_sql(sql=query, con=con)

        payments_df = stage_df[['payment']].copy()
        payments_df.rename(columns={'payment': 'payment_method'}, inplace=True)
        payments_df.drop_duplicates(inplace=True)

        new_payments = payments_df.merge(exists_payments_df, how='left', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1)[payments_df.columns]
        new_payments['start_ts'] = date.today()
        new_payments['end_ts'] = datetime.strptime('01/01/5999', "%m/%d/%Y")
        new_payments['is_current'] = True
        new_payments['version'] = 1

        new_payments.to_sql(name='payments', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        # products
        query = text(f"select {', '.join(target_tables['products'])} from {target_schema}.products")
        exists_products_df = pd.read_sql(sql=query, con=con)

        products_df = stage_df[['product_line']].copy()
        products_df.drop_duplicates(inplace=True)

        new_products = products_df.merge(exists_products_df, how='left', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1)[products_df.columns]
        new_products['start_ts'] = date.today()
        new_products['end_ts'] = datetime.strptime('01/01/5999', "%m/%d/%Y")
        new_products['is_current'] = True
        new_products['version'] = 1

        new_products.to_sql(name='products', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        db_disconnect(con)
        
    def stage_4(stage_df, target_schema, target_tables):
        con = db_connect(hostname, login, password, db)

        # invoice_branches
        query = text(f"select branch_id, {', '.join(target_tables['branches'])} from {target_schema}.branches")
        exists_branches_df = pd.read_sql(sql=query, con=con)

        new_invoice_branches = stage_df[['invoice_id', 'branch', 'city']].copy()
        new_invoice_branches = new_invoice_branches.merge(exists_branches_df, left_on=['branch', 'city'], right_on=['branch_name', 'city'], how='left')
        new_invoice_branches = new_invoice_branches[['invoice_id', 'branch_id']]

        new_invoice_branches.to_sql(name='invoice_branches', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        # invoice_customers
        query = text(f"select customer_id, {', '.join(target_tables['customers'])} from {target_schema}.customers")
        exists_customers_df = pd.read_sql(sql=query, con=con)

        new_invoice_customers = stage_df[['invoice_id', 'customer_type', 'gender']].copy()
        new_invoice_customers = new_invoice_customers.merge(exists_customers_df, left_on=['customer_type', 'gender'], right_on=['customer_type', 'gender'], how='left')
        new_invoice_customers = new_invoice_customers[['invoice_id', 'customer_id']]

        new_invoice_customers.to_sql(name='invoice_customers', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        #invoice_payments
        query = text(f"select payment_id, {', '.join(target_tables['payments'])} from {target_schema}.payments")
        exists_payments_df = pd.read_sql(sql=query, con=con)

        new_invoice_payments = stage_df[['invoice_id', 'payment']].copy()
        new_invoice_payments = new_invoice_payments.merge(exists_payments_df, left_on=['payment'], right_on=['payment_method'], how='left')
        new_invoice_payments = new_invoice_payments[['invoice_id', 'payment_id']]

        new_invoice_payments.to_sql(name='invoice_payments', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        #invoice_products
        query = text(f"select product_id, {', '.join(target_tables['products'])} from {target_schema}.products")
        exists_products_df = pd.read_sql(sql=query, con=con)

        new_invoice_products = stage_df[['invoice_id', 'product_line']].copy()
        new_invoice_products = new_invoice_products.merge(exists_products_df, left_on=['product_line'], right_on=['product_line'], how='left')
        new_invoice_products = new_invoice_products[['invoice_id', 'product_id']]

        new_invoice_products.to_sql(name='invoice_products', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        db_disconnect(con)

    def etl(hostname
            , login
            , password
            , db
            , source_schema
            , source_table
            , target_schema
            , target_tables
            , yesterday):

        con = db_connect(hostname, login, password, db)
        query = text(f"select * from {source_schema}.{source_table} where date = to_date('{yesterday}', 'MM/DD/YYYY')")
        stage_df = pd.read_sql(sql=query, con=con)
        
        db_disconnect(con)
        
        stage_1(stage_df, target_schema, target_tables)
        stage_2(stage_df, target_schema, target_tables)
        stage_3(stage_df, target_schema, target_tables)
        stage_4(stage_df, target_schema, target_tables)

    if is_manual == 1:
        today = datetime.strptime(today, "%m/%d/%Y")
    else:
        today = date.today()
    yesterday = today - timedelta(days = 1)
    yesterday = yesterday.strftime("%-m/%-d/%Y")

    try:
        etl(hostname
            , login
            , password
            , db
            , source_schema
            , source_table
            , target_schema
            , target_tables
            , yesterday)
    except Exception as e:
        print(e)
