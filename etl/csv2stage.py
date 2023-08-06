import pandas as pd
from os import path
from sqlalchemy import create_engine
from datetime import date, timedelta, datetime


def csv2stage_task(filepath
                , hostname
                , login
                , password
                , db
                , target_schema
                , target_table
                , today
                , is_manual):

    def db_connect(hostname, login, password, db):
        engine = create_engine(f'postgresql://{login}:{password}@{hostname}/{db}')
        con = engine.connect()
        return con
    
    def db_disconnect(con):
        con.close()

    def etl(filepath
            , hostname
            , login
            , password
            , db
            , target_schema
            , target_table
            , yesterday):

        csv_df = pd.read_csv(f'{filepath}')

        cols = {
                'Invoice ID':'invoice_id'
                , 'Branch':'branch'
                , 'City':'city'
                , 'Customer type':'customer_type'
                , 'Gender':'gender'
                , 'Product line':'product_line'
                , 'Unit price':'unit_price'
                , 'Quantity':'quantity'
                , 'Tax 5%':'tax_5perc'
                , 'Total':'total'
                , 'Date':'date'
                , 'Time':'time'
                , 'Payment':'payment'
                , 'cogs':'cogs'
                , 'gross margin percentage':'gross_margin_perc'
                , 'gross income':'gross_income'
                , 'Rating':'rating'
        }

        types = {
                'unit_price':'float64'
                , 'quantity':'int32'
                , 'tax_5perc':'float64'
                , 'total':'float64'
                , 'cogs':'float64'
                , 'gross_margin_perc':'float64'
                , 'gross_income':'float64'
                , 'rating':'float64'
        }

        csv_df.rename(columns=cols, inplace=True)
        csv_df.astype(types)
        
        csv_df = csv_df[csv_df.date == yesterday]

        con = db_connect(hostname, login, password, db)
        csv_df.to_sql(name=target_table, con=con, schema=target_schema, if_exists='append', index=False)
        # con.commit()
        db_disconnect(con)
    
    if is_manual == 1:
        today = datetime.strptime(today, "%m/%d/%Y")
    else:
        today = date.today()
    yesterday = today - timedelta(days = 1)
    yesterday = yesterday.strftime("%-m/%-d/%Y")
    
    try:
        etl(filepath
            , hostname
            , login
            , password
            , db
            , target_schema
            , target_table
            , yesterday)
    except Exception as e:
        print(e)
