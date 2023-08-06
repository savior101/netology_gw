/*Создадим схемы наших слоев*/


create schema stage;

create schema nds;

create schema dds;


/*Создадим таблицы*/

/*stage*/
create table stage.supermarket_sales (
	invoice_id				varchar(15)		primary key
	, branch				varchar(50)
	, city					varchar(50)
	, customer_type			varchar(25)
	, gender				varchar(25)
	, product_line			varchar(100)
	, unit_price			numeric
	, quantity				int2
	, tax_5perc				numeric
	, total					numeric
	, "date"				date
	, "time"				time
	, payment				varchar(25)
	, cogs					numeric
	, gross_margin_perc		numeric
	, gross_income			numeric
	, rating				numeric
);

/*nds*/
create table nds.dict_gross_margin_perc (
	gross_margin_perc_id	serial			primary key
	, gross_margin_perc		numeric
);

create table nds.invoices (
	invoice_id				varchar(15)		primary key
	, unit_price			numeric
	, quantity				int2
	, tax					numeric
	, total					numeric
	, "date"				date
	, "time"				time
	, cogs					numeric
	, gross_margin_perc_id	int2
	, gross_income			numeric
	, rating				numeric
	, constraint invoices_gmp_fk foreign key (gross_margin_perc_id) references nds.dict_gross_margin_perc (gross_margin_perc_id)
);

create table nds.branches (
	branch_id				serial			primary key
	, branch_name			varchar(50)
	, city					varchar(50)
	, start_ts				date
	, end_ts				date
	, is_current			boolean
	, "version"				int2
);

create table nds.invoice_branches (
	invoice_id				varchar(15)		
	, branch_id				int
	, constraint invoice_branches_pk primary key (invoice_id, branch_id)
	, constraint invoice_branches_i_fk foreign key (invoice_id) references nds.invoices (invoice_id)
 	, constraint invoice_branches_b_fk foreign key (branch_id) references nds.branches (branch_id)
);

create table nds.customers (
	customer_id				serial			primary key
	, customer_type			varchar(25)
	, gender				varchar(25)
	, start_ts				date
	, end_ts				date
	, is_current			boolean
	, "version"				int2
);

create table nds.invoice_customers (
	invoice_id				varchar(15)		
	, customer_id			int
	, constraint invoice_customers_pk primary key (invoice_id, customer_id)
	, constraint invoice_customers_i_fk foreign key (invoice_id) references nds.invoices (invoice_id)
 	, constraint invoice_customers_c_fk foreign key (customer_id) references nds.customers (customer_id)
);

create table nds.payments (
	payment_id				serial			primary key
	, payment_method		varchar(25)
	, start_ts				date
	, end_ts				date
	, is_current			boolean
	, "version"				int2
);

create table nds.invoice_payments (
	invoice_id				varchar(15)		
	, payment_id			int
	, constraint invoice_payments_pk primary key (invoice_id, payment_id)
	, constraint invoice_payments_i_fk foreign key (invoice_id) references nds.invoices (invoice_id)
 	, constraint invoice_payments_p_fk foreign key (payment_id) references nds.payments (payment_id)
);

create table nds.products (
	product_id				serial			primary key
	, product_line			varchar(100)
	, start_ts				date
	, end_ts				date
	, is_current			boolean
	, "version"				int2
);

create table nds.invoice_products (
	invoice_id				varchar(15)		
	, product_id			int
	, constraint invoice_products_pk primary key (invoice_id, product_id)
	, constraint invoice_products_i_fk foreign key (invoice_id) references nds.invoices (invoice_id)
 	, constraint invoice_products_p_fk foreign key (product_id) references nds.products (product_id)
);


/*dds*/
create table dds.dim_branches (
	branch_id				int				primary key
	, branch_name			varchar(50)
	, city					varchar(50)
);

create table dds.dim_customers (
	customer_id				int				primary key
	, customer_type			varchar(25)
	, gender				varchar(25)
);

create table dds.dim_payments (
	payment_id				int				primary key
	, payment_method		varchar(25)
);

create table dds.dim_products (
	product_id				int				primary key
	, product_line			varchar(100)
);

create table dds.dim_calendar (
	calendar_id				serial			primary key
	, "date"				date
	, "day"					int
	, week_number			int
	, "month"				int
	, "year"				int
	, week_day				int
	, is_holyday			boolean
);

create table dds.fact_sales (
	invoice_id				varchar(15)
	, branch_id				int
	, customer_id			int
	, payment_id			int
	, product_id			int
	, calendar_id			int
	, unit_price			numeric
	, quantity				int2
	, tax					numeric
	, total					numeric
	, "time"				time
	, cogs					numeric
	, gross_margin_perc		numeric
	, gross_income			numeric
	, rating				numeric
	, constraint fact_sales_pk primary key (invoice_id, branch_id, customer_id, payment_id, product_id, calendar_id)
	, constraint fact_sales_b_fk foreign key (branch_id) references dds.dim_branches (branch_id)
	, constraint fact_sales_c_fk foreign key (customer_id) references dds.dim_customers (customer_id)
	, constraint fact_sales_p_fk foreign key (payment_id) references dds.dim_payments (payment_id)
	, constraint fact_sales_pr_fk foreign key (product_id) references dds.dim_products (product_id)
	, constraint fact_sales_cl_fk foreign key (calendar_id) references dds.dim_calendar (calendar_id)
);

