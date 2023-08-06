import pandas as pd
from os import path
from sqlalchemy import create_engine, text
from datetime import date, timedelta, datetime


def nds2dds_task(hostname
                , login
                , password
                , db
                , source_schema
                , source_tables
                , target_schema
                , target_tables
                , holydays
                , today
                , is_manual):
    
    def db_connect(hostname, login, password, db):
        engine = create_engine(f'postgresql://{login}:{password}@{hostname}/{db}')
        con = engine.connect()
        return con
    
    def db_disconnect(con):
        con.close()
    
    def stage_1(nds_df, target_schema, target_tables, holydays):
        con = db_connect(hostname, login, password, db)

        # dim_branches
        query = text(f"select {', '.join(target_tables['dim_branches'])} from {target_schema}.dim_branches")
        exists_branches_df = pd.read_sql(sql=query, con=con)

        branches_df = nds_df[target_tables['dim_branches']].copy()
        branches_df.drop_duplicates(inplace=True)

        new_branches = branches_df.merge(exists_branches_df, how='left', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1)[branches_df.columns]
        new_branches.to_sql(name='dim_branches', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        # dim_customers
        query = text(f"select {', '.join(target_tables['dim_customers'])} from {target_schema}.dim_customers")
        exists_customers_df = pd.read_sql(sql=query, con=con)
        
        customers_df = nds_df[target_tables['dim_customers']].copy()
        customers_df.drop_duplicates(inplace=True)

        new_customers = customers_df.merge(exists_customers_df, how='left', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1)[customers_df.columns]
        new_customers.to_sql(name='dim_customers', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        # dim_payments
        query = text(f"select {', '.join(target_tables['dim_payments'])} from {target_schema}.dim_payments")
        exists_payments_df = pd.read_sql(sql=query, con=con)

        payments_df = nds_df[target_tables['dim_payments']].copy()
        payments_df.drop_duplicates(inplace=True)

        new_payments = payments_df.merge(exists_payments_df, how='left', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1)[payments_df.columns]
        new_payments.to_sql(name='dim_payments', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        # dim_products
        query = text(f"select {', '.join(target_tables['dim_products'])} from {target_schema}.dim_products")
        exists_products_df = pd.read_sql(sql=query, con=con)

        products_df = nds_df[target_tables['dim_products']].copy()
        products_df.drop_duplicates(inplace=True)

        new_products = products_df.merge(exists_products_df, how='left', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1)[products_df.columns]
        new_products.to_sql(name='dim_products', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        # dim_calendar
        d_holydays = [datetime.strptime(x, "%m/%d/%Y").date() for x in holydays]

        query = text(f"select date from {target_schema}.dim_calendar")
        exists_calendar_df = pd.read_sql(sql=query, con=con)

        calendar_df = nds_df[['date']].copy()
        calendar_df.drop_duplicates(inplace=True)

        new_calendar = calendar_df.merge(exists_calendar_df, how='left', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1)[calendar_df.columns]
        new_calendar['day'] = datetime.strptime(yesterday, "%m/%d/%Y").timetuple().tm_yday
        new_calendar['week_number'] = datetime.strptime(yesterday, "%m/%d/%Y").isocalendar()[1]
        new_calendar['month'] = datetime.strptime(yesterday, "%m/%d/%Y").month
        new_calendar['year'] = datetime.strptime(yesterday, "%m/%d/%Y").year
        new_calendar['week_day'] = datetime.strptime(yesterday, "%m/%d/%Y").isoweekday()
        new_calendar['is_holyday'] = new_calendar.apply(lambda x: True if x['date'] in d_holydays else False, axis=1)
        new_calendar.to_sql(name='dim_calendar', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        db_disconnect(con)
    
    def stage_2(nds_df, target_schema, target_tables):
        con = db_connect(hostname, login, password, db)

        # fact_sales
        f_sales_df = nds_df[['invoice_id'
                        , 'branch_id'
                        , 'customer_id'
                        , 'payment_id'
                        , 'product_id'
                        , 'date'
                        , 'unit_price'
                        , 'quantity'
                        , 'tax'
                        , 'total'
                        , 'time'
                        , 'cogs'
                        , 'gross_margin_perc'
                        , 'gross_income'
                        , 'rating']].copy()
        
        query = text(f"select calendar_id, date from {target_schema}.dim_calendar")
        exists_calendar_df = pd.read_sql(sql=query, con=con)

        f_sales_df = f_sales_df.merge(exists_calendar_df, on='date', how='left')
        f_sales_df = f_sales_df.loc[:, f_sales_df.columns!='date']

        f_sales_df.to_sql(name='fact_sales', con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()

        db_disconnect(con)
    

    def etl(hostname
            , login
            , password
            , db
            , source_schema
            , source_tables
            , target_schema
            , target_tables
            , holydays
            , yesterday):

        con = db_connect(hostname, login, password, db)
        query = text(
            f'''
            select
                i.invoice_id 
                , b.branch_name 
                , b.city 
                , c.customer_type 
                , c.gender 
                , p.product_line 
                , i.unit_price 
                , i.quantity 
                , i.tax 
                , i.total 
                , i."date" 
                , i."time" 
                , p2.payment_method 
                , i.cogs
                , dgmp.gross_margin_perc 
                , i.gross_income 
                , i.rating
                , b.branch_id
                , c.customer_id
                , p.product_id
                , p2.payment_id
            from {source_schema}.invoices i 
            left join {source_schema}.invoice_branches ib on ib.invoice_id = i.invoice_id 
            left join {source_schema}.branches b on b.branch_id = ib.branch_id 
            left join {source_schema}.invoice_customers ic on ic.invoice_id = i.invoice_id 
            left join {source_schema}.customers c on c.customer_id = ic.customer_id 
            left join {source_schema}.invoice_products ip on ip.invoice_id = i.invoice_id 
            left join {source_schema}.products p on p.product_id = ip.product_id 
            left join {source_schema}.invoice_payments ip2 on ip2.invoice_id = i.invoice_id 
            left join {source_schema}.payments p2 on p2.payment_id = ip2.payment_id 
            left join {source_schema}.dict_gross_margin_perc dgmp on dgmp.gross_margin_perc_id = i.gross_margin_perc_id
            where i.date = to_date('{yesterday}', 'MM/DD/YYYY')
            '''
        )
        nds_df = pd.read_sql(sql=query, con=con)
        
        db_disconnect(con)
        
        stage_1(nds_df, target_schema, target_tables, holydays)
        stage_2(nds_df, target_schema, target_tables)

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
            , source_tables
            , target_schema
            , target_tables
            , holydays
            , yesterday)
    except Exception as e:
        print(e)
