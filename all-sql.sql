CREATE TABLE symbols 
(
  stock_id int NOT NULL AUTO_INCREMENT,
  symbol varchar(10) NOT NULL,
  company varchar(255) NULL,
  exchange varchar(50) NULL,
	PRIMARY KEY (stock_id)
);


CREATE PROCEDURE p_GetCompany

DELIMITER //

CREATE PROCEDURE get_company_by_symbol
(
	symbol VARCHAR(8)
)
BEGIN

SELECT s.company FROM symbols as s WHERE s.symbol = symbol;

END//

DELIMITER;


DELIMITER //
 CREATE PROCEDURE get_exchange_by_symbol
     (
     symbol VARCHAR(8)
     )
     BEGIN
     
     SELECT s.exchange FROM symbols as s WHERE s.symbol = symbol;
     
     END//
DELIMITER ;

CREATE TABLE other_listed / not used /
(
	other_stock_id int not null AUTO_INCREMENT,
	ACT_Symbol VARCHAR,
	Company_Name VARCHAR(255),
	Security_Name VARCHAR(255),
	Exchange CHAR(1),
	CQS_Symbol VARCHAR(8),
	ETF CHAR(1),
	Round_Lot_Size INT NULL,
	Test_Issue CHAR(1),
	NASDAQ_Symbol varchar(
)

CREATE TABLE exchange_industry  -- not used
(
   ei_id int not null auto_increment,
   ticker varchar(8),
   exchange_name varchar(10),
   sector varchar(100),
   industry varchar(100),
   primary_key (ei_id)
 );
 
 
 CREATE TABLE exchange
 (
     exchange_id int not null auto_increment,
	 exchange varchar(10),
	 primary key (exchange_id)
 );

insert into exchange (exchange) select 'NASDAQ' union all select 'NYSE'

ALTER TABLE symbol ADD CONSTRAINT fk_exchange_id FOREIGN KEY (exchange_id) REFERENCES exchange(exchange_id);

CREATE VIEW stock 
SELECT s.stock_id, s.symbol, s.company, e.exchange
FROM symbol as s
JOIN exchange as e ON s.exchange_id = e.exchange_id
	
	create table historical_stocks
	( id int not null auto_increment,
	  ticker varchar(50),
	  exchange varchar(10),
	  name varchar(255),
	  sector varchar(50),
	  industry varchar(255),
	  primary key  (id)
	  );
	  create index ix_ticker on historical_stocks (ticker);
	  create index ix_industry on historical_stocks (sector, industry);
	  create index ix_exchange on historical_stocks (exchange);
	  
	  UPDATE symbol
    INNER JOIN historical_stocks_staging 
     ON symbol.symbol  = historical_stocks_staging.ticker
	 INNER JOIN industry ON historical_stocks_staging.sector = industry.sector AND historical_stocks_staging.industry = industry.industry
SET 
    symbol.industry_id = industry.industry_id;


   INSERT INTO symbol (symbol, company, exchange_id, industry_id) 
   SELECT h.ticker, 
   				h.name as company, 
				CASE WHEN h.exchange = 'NASDAQ' THEN 1 ELSE 2 END as exchange, 
				i.industry_id
	FROM historical_stocks_staging as h
	INNER JOIN industry as i ON h.sector = i.sector AND h.industry = i.industry
	WHERE h.ticker NOT IN (SELECT symbol FROM symbol);
	
	ALTER TABLE symbol ADD CONSTRAINT fk_industry_id FOREIGN KEY (industry_id) REFERENCES industry(industry_id);
	
	ALTER VIEW stock AS
SELECT s.stock_id, s.symbol, s.company, e.exchange, i.sector, i.industry
FROM symbol as s
JOIN exchange as e ON s.exchange_id = e.exchange_id
LEFT JOIN industry as i ON s.industry_id = i.industry_id;


create table dim_day (
 day_key int not null auto_increment,   
 full_date datetime,     
 date_num int,   
 day_num int,   
 day_of_year int, 
 day_of_week int, 
 day_of_week_name varchar(20), 
 week_num int , 
 week_begin_date datetime, 
 week_end_date datetime, 
 last_week_begin_date datetime, 
 last_week_end_date datetime,   
 last_2_week_begin_date  datetime,  
 last_2_week_end_date datetime,
 month_num int,  
 month_name varchar(20),  
 yearmonth_num int,  
 last_month_num int , 
 last_month_name varchar(20), 
 last_month_year int,  
 last_yearmonth_num int, 
 quarter_num int ,  
 year_num int, 
 created_date timestamp not null ,  
 updated_date timestamp not null, 
 primary key (day_key)
 );
 
 delimiter //

CREATE PROCEDURE sp_day_dim (in p_start_date datetime, p_end_date datetime)
BEGIN

  Declare StartDate datetime;
  Declare EndDate datetime;
  Declare RunDate datetime;

-- Set date variables

  Set StartDate = p_start_date; -- update this value to reflect the earliest date that you will use.
  Set EndDate = p_end_date; -- update this value to reflect the latest date that you will use.
  Set RunDate = StartDate;

-- Loop through each date and insert into DimTime table

WHILE RunDate <= EndDate DO

INSERT Into dim_day(
 full_date ,
 date_num,
 day_num ,
 Day_of_Year,
 Day_of_Week,
 Day_of_week_name,
 Week_num,
 week_begin_date,
 week_end_date,
 last_week_begin_date,
 last_week_end_date,
 last_2_week_begin_date,
 last_2_week_end_date,
 Month_num ,
 Month_Name,
 yearmonth_num,
 last_month_num,
 last_month_name,
 last_month_year,
 last_yearmonth_num,
 Quarter_num ,
 Year_num  ,
 created_date, 
 updated_date 
)
select 
RunDate date
,CONCAT(year(RunDate), lpad(MONTH(RunDate),2,'0'),lpad(day(RunDate),2,'0')) date_num
,day(RunDate) day_num
,DAYOFYEAR(RunDate) day_of_year
,DAYOFWEEK(RunDate) day_of_week
,DAYNAME(RunDate) day_of_week_name
,WEEK(RunDate) week_num
,DATE_ADD(RunDate, INTERVAL(1-DAYOFWEEK(RunDate)) DAY) week_begin_date
,ADDTIME(DATE_ADD(RunDate, INTERVAL(7-DAYOFWEEK(RunDate)) DAY),'23:59:59') week_end_date
,DATE_ADD(RunDate, INTERVAL ((1-DAYOFWEEK(RunDate))-7) DAY) last_week_begin_date
,ADDTIME(DATE_ADD(RunDate, INTERVAL ((7-DAYOFWEEK(RunDate))-7) DAY),'23:59:59')last_week_end_date
,DATE_ADD(RunDate, INTERVAL ((1-DAYOFWEEK(RunDate))-14) DAY) last_2_week_begin_date
,ADDTIME(DATE_ADD(RunDate, INTERVAL ((7-DAYOFWEEK(RunDate))-7) DAY),'23:59:59')last_2_week_end_date
,MONTH(RunDate) month_num
,MONTHNAME(RunDate) month_name
,CONCAT(year(RunDate), lpad(MONTH(RunDate),2,'0')) YEARMONTH_NUM
,MONTH(date_add(RunDate,interval -1 month)) last_month_num
,MONTHNAME(date_add(RunDate,interval -1 month)) last_month_name
,year(date_add(RunDate,interval -1 month)) last_month_year
,CONCAT(year(date_add(RunDate,interval -1 month)),lpad(MONTH(date_add(RunDate,interval -1 month)),2,'0')) Last_YEARMONTH_NUM
,QUARTER(RunDate) quarter_num
,YEAR(RunDate) year_num
,now() created_date
,now() update_date
;
-- commit;
-- increase the value of the @date variable by 1 day

Set RunDate = ADDDATE(RunDate,1);

END WHILE;
commit;
END;
//
delimiter ;

'1000-01-01 00:00:00' 

CALL sp_day_dim ('1950-01-01 00:00:00', '2050-12-31 00:00:00');  

CREATE TABLE price_hist
(
   price_hist_id int not null auto_increment,
   symbol varchar(10),
   open_price DECIMAL(10,2),
   close_price DECIMAL(10,2),
   adj_close DECIMAL(14,6),
   low_price DECIMAL(10,2),
   high_price DECIMAL(10,2),
   volume int,
   trading_dte DATETIME ,
   PRIMARY KEY(price_hist_id)
);

Excel insert generation template
="INSERT INTO price_hist (symbol,open_price,close_price,adj_close,low_price,high_price,volume,trading_dte) VALUES ('"&A2&"',"&B2&","&C2&","&D2&","&E2&","&F2&","&G2&",'"&H2&"');"


wget --output-document price_hist.zip https://github.com/RobertHolzhauser/E59/blob/main/price_hist.zip

CREATE VIEW overall_market_minmax
AS
SELECT MIN(trading_dte) as min_date, MAX(trading_dte) as max_date, MIN(open_price) as min_open,MAX(open_price) as max_open,	MIN(close_price) as min_close,MAX(close_price) as max_close, 
				MIN(low_price) min_low, 
				MAX(low_price) as max_low,
				MIN(high_price) as min_high,
				MAX(high_price) as max_high
FROM price_hist;

CREATE VIEW overall_market_minmax_by_year
AS
SELECT YEAR(trading_dte) as year, MIN(open_price) as min_open,MAX(open_price) as max_open,	MIN(close_price) as min_close,MAX(close_price) as max_close, 
				MIN(low_price) min_low, 
				MAX(low_price) as max_low,
				MIN(high_price) as min_high,
				MAX(high_price) as max_high
FROM price_hist
GROUP BY YEAR(trading_dte);

CREATE VIEW overall_market_avg
AS
SELECT MIN(trading_dte) as min_date, MAX(trading_dte) as max_date, 
			    AVG(open_price) as avg_open,
				AVG(close_price) as avg_close,
				AVG(low_price) avg_low, 
				AVG(high_price) as avg_high
FROM price_hist;

CREATE VIEW overall_market_avg_by_year
AS
SELECT YEAR(trading_dte) as year, 
			   AVG(open_price) as avg_open, 
			   AVG(close_price) as avg_close,
			   AVG(low_price) avg_low, 
			   AVG(high_price) as avg_high
FROM price_hist
GROUP BY YEAR(trading_dte);

CREATE VIEW overall_exchange_minmax
AS
SELECT e.exchange, MIN(trading_dte) as min_date, MAX(trading_dte) as max_date, MIN(open_price) as min_open,MAX(open_price) as max_open,	MIN(close_price) as min_close,MAX(close_price) as max_close, 
				MIN(low_price) min_low, 
				MAX(low_price) as max_low,
				MIN(high_price) as min_high,
				MAX(high_price) as max_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN exchange as e ON s.exchange_id = e.exchange_id
GROUP BY e.exchange;

CREATE VIEW overall_exchange_avg
AS
SELECT e.exchange, MIN(trading_dte) as min_date, 
				MAX(trading_dte) as max_date,    
				AVG(open_price) as avg_open,	
				AVG(close_price) as avg_close,
				AVG(low_price) as avg_low, 
				AVG(high_price) as avg_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN exchange as e ON s.exchange_id = e.exchange_id
GROUP BY e.exchange;

CREATE VIEW overall_exchange_avg_by_year
AS
SELECT e.exchange,  YEAR(trading_dte) AS year,  
				AVG(open_price) as avg_open,	
				AVG(close_price) as avg_close,
				AVG(low_price) as avg_low, 
				AVG(high_price) as avg_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN exchange as e ON s.exchange_id = e.exchange_id
GROUP BY e.exchange, YEAR(trading_dte) ;

CREATE VIEW overall_exchange_minmax_by_year
AS
SELECT e.exchange,  YEAR(trading_dte) AS year,  
				MIN(open_price) as min_open,	
				MAX(open_price) as max_open,	
				MIN(close_price) as min_close,
				MAX(close_price) as max_close,
				MIN(low_price) as min_low, 
				MAX(low_price) as max_low, 
				MIN(high_price) as min_high,
				MAX(high_price) as max_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN exchange as e ON s.exchange_id = e.exchange_id
GROUP BY e.exchange, YEAR(trading_dte) ;

CREATE VIEW overall_sector_minmax
AS
SELECT i.sector, MIN(trading_dte) as min_date, MAX(trading_dte) as max_date, MIN(open_price) as min_open,MAX(open_price) as max_open,	MIN(close_price) as min_close,MAX(close_price) as max_close, 
				MIN(low_price) min_low, 
				MAX(low_price) as max_low,
				MIN(high_price) as min_high,
				MAX(high_price) as max_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY i.sector;

CREATE VIEW overall_sector_avg
AS
SELECT i.sector,   
				AVG(open_price) as avg_open,	
				AVG(close_price) as avg_close,
				AVG(low_price) as avg_low, 
				AVG(high_price) as avg_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY i.sector;

CREATE VIEW overall_exchange_avg_by_year
AS
SELECT e.exchange,  YEAR(trading_dte) AS year,  
				AVG(open_price) as avg_open,	
				AVG(close_price) as avg_close,
				AVG(low_price) as avg_low, 
				AVG(high_price) as avg_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN exchange as e ON s.exchange_id = e.exchange_id
GROUP BY e.exchange, YEAR(trading_dte) ;

CREATE VIEW overall_exchange_count_by_sector
AS
SELECT e.exchange,  i.sector, count(distinct s.symbol) as stocks
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN exchange as e ON s.exchange_id = e.exchange_id
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY e.exchange, i.sector ;

CREATE VIEW overall_exchange_count_by_industry
AS
SELECT e.exchange,  i.industry, count(distinct s.symbol) as stocks
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN exchange as e ON s.exchange_id = e.exchange_id
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY e.exchange, i.industry ;

CREATE VIEW overall_industry_minmax
AS
SELECT i.industry, MIN(trading_dte) as min_date, MAX(trading_dte) as max_date, MIN(open_price) as min_open,MAX(open_price) as max_open,	MIN(close_price) as min_close,MAX(close_price) as max_close, 
				MIN(low_price) min_low, 
				MAX(low_price) as max_low,
				MIN(high_price) as min_high,
				MAX(high_price) as max_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY i.industry;

CREATE VIEW overall_industry_averages
AS
SELECT i.industry, MIN(trading_dte) as min_date, 
			    AVG(open_price) as avg_open,
				AVG(close_price) as avg_close,
				AVG(low_price) avg_low, 
				AVG(high_price) as avg_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY i.industry;

CREATE VIEW overall_sector_minmax_by_year
AS
SELECT YEAR(p.trading_dte) as year,
				i.sector,   
			    MIN(open_price) as min_open,
				MAX(open_price) as max_open,	
				MIN(close_price) as min_close,
				MAX(close_price) as max_close, 
				MIN(low_price) min_low, 
				MAX(low_price) as max_low,
				MIN(high_price) as min_high,
				MAX(high_price) as max_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY  YEAR(p.trading_dte), i.sector;

CREATE VIEW overall_sector_minmax_by_year
AS
SELECT YEAR(p.trading_dte) as year,
				i.sector,   
			    MIN(open_price) as min_open,
				MAX(open_price) as max_open,	
				MIN(close_price) as min_close,
				MAX(close_price) as max_close, 
				MIN(low_price) min_low, 
				MAX(low_price) as max_low,
				MIN(high_price) as min_high,
				MAX(high_price) as max_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY  YEAR(p.trading_dte), i.sector;

CREATE VIEW overall_sector_avg_by_year
AS
SELECT YEAR(p.trading_dte) as year,
				i.sector,   
			    AVG(open_price) as avg_open,
				AVG(close_price) as min_close,
				AVG(low_price) min_low, 
				AVG(high_price) as min_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY  YEAR(p.trading_dte), i.sector;

CREATE VIEW overall_industry_minmax_by_year
AS
SELECT YEAR(p.trading_dte) as year,
				i.industry,   
			    MIN(open_price) as min_open,
				MAX(open_price) as max_open,	
				MIN(close_price) as min_close,
				MAX(close_price) as max_close, 
				MIN(low_price) min_low, 
				MAX(low_price) as max_low,
				MIN(high_price) as min_high,
				MAX(high_price) as max_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY  YEAR(p.trading_dte), i.industry;

CREATE VIEW overall_industry_avg_by_year
AS
SELECT YEAR(p.trading_dte) as year,
				i.industry,   
			    AVG(open_price) as avg_open,
				AVG(close_price) as min_close,
				AVG(low_price) min_low, 
				AVG(high_price) as min_high
FROM price_hist as p 
LEFT JOIN symbol as s ON p.symbol = s.symbol 
LEFT JOIN industry as i ON s.industry_id = i.industry_id
GROUP BY  YEAR(p.trading_dte), i.industry;


CREATE VIEW company_minmaxavg
AS
SELECT
				s.symbol, s.company, min(low_price) as min_low, max(high_price) as max_high, avg(low_price) as avg_low, avg(high_price) as avg_high
FROM price_hist as p 
LEFT OUTER JOIN symbol as s ON p.symbol = s.symbol 
GROUP BY  s.symbol, s.company;

CREATE VIEW company_minmaxavg_by_year
AS
SELECT
				d.year_num,  s.symbol, s.company, min(low_price) as min_low, max(high_price) as max_high, avg(low_price) as avg_low, avg(high_price) as avg_high
FROM price_hist as p 
LEFT OUTER JOIN symbol as s ON p.symbol = s.symbol 
JOIN dim_day AS d ON p.trading_dte = d.full_date 
GROUP BY  d.year_num, s.symbol, s.company;

CREATE VIEW company_minmaxavg_by_month
AS
SELECT
				d.yearmonth_num, s.symbol, s.company, min(low_price) as min_low, max(high_price) as max_high, avg(low_price) as avg_low, avg(high_price) as avg_high
FROM price_hist as p 
LEFT OUTER JOIN symbol as s ON p.symbol = s.symbol 
JOIN dim_day AS d ON p.trading_dte = d.full_date 
GROUP BY  d.yearmonth_num,  s.symbol, s.company;

CREATE TABLE phase1_opportunity
(
   phase1_id int not null auto_increment,
   symbol varchar(10) not null,
   avg_spread decimal(10,2),
   avg_pcnt_spread decimal(10,2),
   nbr_mos int,   /* that the pattern exists */
   rolling_average_trend decimal(6,2), 
   primary key (phase1_id)
);

DELIMITER //
CREATE PROCEDURE find_phase1_oppty()
BEGIN
	
	/* truncate phase 1 table to start fresh */
	DELETE FROM phase1_opportunity;  

	/* step 1 - identify all stocks that have at least 5 months with a low, difference high between 10% to 30% */
	INSERT INTO phase1_opportunity (symbol, nbr_mos, avg_spread, avg_pcnt_spread )
	SELECT m.symbol, 
					COUNT(1) as nbr_mos, 
					AVG(m.max_high - m.min_low) as avg_spread,
					AVG(m.min_low / m.max_high )  as avg_pcnt_spread
	FROM company_minmaxavg_by_month as m
	WHERE (m.max_high - m.min_low) >= (m.max_high * 0.1) 
		AND (m.max_high - m.min_low) <= (m.max_high * 0.3)
	GROUP BY m.symbol
	HAVING COUNT(1) >= 5;
	
END //
DELIMITER ;

CREATE VIEW phase1_info20
AS
SELECT s.*, p.nbr_mos
FROM stock as s JOIN phase1_opportunity as p ON s.symbol = p.symbol
ORDER BY p.nbr_mos DESC
LIMIT 20;

CREATE VIEW phase1_info20_yearly
AS
SELECT p.symbol, p.company, c.year_num, c.min_low, c.max_high, (c.avg_low + c.avg_high) / 2 as avg_price
FROM  phase1_info20 as p 
JOIN company_minmaxavg_by_year as c ON c.symbol = p.symbol;

CREATE VIEW phase1_info20_monthly
AS
SELECT p.symbol, p.company, c.yearmonth_num, c.min_low, c.max_high, (c.avg_low + c.avg_high) / 2 as avg_price
FROM  phase1_info20 as p 
JOIN company_minmaxavg_by_month as c ON c.symbol = p.symbol
ORDER BY p.symbol, yearmonth_num DESC;

CREATE VIEW phase1_info20_daily
AS
SELECT p.symbol, p.company, d.date_num, h.low_price, h.high_price, (h.low_price + h.high_price + h.open_price + h.close_price) / 4 as avg_price
FROM  phase1_info20 as p 
JOIN price_hist as h ON p.symbol = h.symbol
JOIN dim_day as d ON h.trading_dte = d.full_date
ORDER BY p.symbol, d.date_num DESC;
