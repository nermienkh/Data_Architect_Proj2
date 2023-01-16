-- ods tables

create or replace TABLE UDACITYPROJECT.ODS.PRECIPITATION (
	DATE_T DATE,
	PRECIPITATION FLOAT,
	PRECIPITATION_NORMAL FLOAT,
	primary key (DATE_T)
);


create or replace TABLE UDACITYPROJECT.ODS.TEMPERATURE (
	DATE_T DATE,
	MIN_T NUMBER(38,0),
	MAX_T NUMBER(38,0),
	NORMAL_MIN FLOAT,
	NORMAL_MAX FLOAT,
	primary key (DATE_T)
);

CREATE TABLE business (
	business_id string,
	name string,
	address string,
	city string,
	state string,
	postal_code string,
	latitude FLOAT,
	longitude FLOAT,
	stars FLOAT,
	review_count NUMBER,
	is_open NUMBER,
	attribute VARIANT,
	categories VARCHAR,
	hours VARIANT, primary key (business_id));



CREATE TABLE user (
	average_stars FLOAT,
	compliment_cool NUMBER,
	compliment_cute NUMBER,
	compliment_funny NUMBER,
	compliment_hot NUMBER,
	compliment_list NUMBER,
	compliment_more NUMBER,
	compliment_note NUMBER,
	compliment_photos NUMBER,
	compliment_plain NUMBER,
	compliment_profile NUMBER,
	compliment_writer NUMBER,
	cool NUMBER,
	elite STRING,
	fans NUMBER,
	friends VARIANT,
	funny NUMBER,
	name VARCHAR,
	review_count NUMBER,
	useful NUMBER,
	user_id string,
	yelping_since STRING,  PRIMARY KEY (user_id));





CREATE TABLE tip (
	business_id STRING REFERENCES business(business_id),
	compliment_count INTEGER,
	date STRING,
	text STRING,
	user_id STRING  REFERENCES user(user_id)); 


create or replace TABLE UDACITYPROJECT.ODS.REVIEW (
	BUSINESS_ID VARCHAR(16777216),
	COOL NUMBER(38,0),
	DATE VARCHAR(16777216),
	FUNNY NUMBER(38,0),
	REVIEW_ID VARIANT,
	STARS NUMBER(38,0),
	TEXT VARCHAR(16777216),
	USEFUL NUMBER(38,0),
	USER_ID VARCHAR(16777216),
	primary key (REVIEW_ID),
	foreign key (BUSINESS_ID) references UDACITYPROJECT.ODS.BUSINESS(BUSINESS_ID),
	foreign key (USER_ID) references UDACITYPROJECT.ODS.USER(USER_ID)
);

CREATE TABLE checkin(
	business_id string  REFERENCES business(business_id),
	date STRING);
 


CREATE TABLE covid (
	Call_To_Action_enabled VARIANT,
	Covid_Banner VARIANT,
	Grubhub_enabled VARIANT,
	Request_a_Quote_Enabled VARIANT,
	Temporary_Closed_Until VARIANT,
	Virtual_Services_Offered VARIANT,
	business_id string,
	delivery_or_takeout VARIANT,
	highlights VARIANT);
ALTER TABLE covid ADD FOREIGN KEY (business_id) REFERENCES business(business_id); 


-------------------------------------------------
-- move data from staging to ods
--1 climate_Precipitation data
-- first we need to replace all "T" values into any number to cast data 
use SCHEMA STAGING; 
UPDATE climate_Precipitation SET precipitation = 999999999 WHERE precipitation = 'T'; 
-- number of rows updated 543  

INSERT INTO precipitation(date_t, precipitation, precipitation_normal)
SELECT TO_DATE(date,'YYYYMMDD'), 
CAST(precipitation AS double), 
precipitation_normal FROM udacityproject.STAGING.climate_Precipitation;

--2  climate_temperature

INSERT INTO temperature(date_t, min_t, max_t, normal_min, normal_max)
SELECT TO_DATE(date, 'YYYYMMDD'),
min ,
max ,
normal_min ,
normal_max FROM udacityproject.STAGING.climate_temperature;

--3 yelp_tip

INSERT INTO tip(business_id, compliment_count, date, text, user_id) 
SELECT parse_json($1):business_id,
			parse_json($1):compliment_count,
			parse_json($1):date,
			parse_json($1):text,
			parse_json($1):user_id
	FROM udacityproject.STAGING.yelp_tip;

--4 yelp_business 

INSERT INTO business(business_id,name, address, city, state, postal_code, latitude,longitude,stars, review_count, is_open, attribute, categories, hours)
SELECT parse_json($1):business_id,
			parse_json($1):name,
			parse_json($1):address,
			parse_json($1):city,
			parse_json($1):state,
			parse_json($1):postal_code,
			parse_json($1):latitude,
			parse_json($1):longitude,
			parse_json($1):stars,
			parse_json($1):review_count,
			parse_json($1):is_open,
			parse_json($1):attribute,
			parse_json($1):categories,
			parse_json($1):hours
	FROM udacityproject.STAGING.yelp_business;

--5 yelp_user

INSERT INTO user(average_stars,compliment_cool,compliment_cute,compliment_funny,compliment_hot,compliment_list,compliment_more,
compliment_note,compliment_photos,compliment_plain,compliment_profile,compliment_writer,cool,elite, fans,friends, funny,name, review_count, useful,user_id,yelping_since)
SELECT parse_json($1):average_stars,
	parse_json($1):compliment_cool,
	parse_json($1):compliment_cute,
	parse_json($1):compliment_funny,
	parse_json($1):compliment_hot,
	parse_json($1):compliment_list,
	parse_json($1):compliment_more,
	parse_json($1):compliment_note,
	parse_json($1):compliment_photos,
	parse_json($1):compliment_plain,
	parse_json($1):compliment_profile,
	parse_json($1):compliment_writer,
	parse_json($1):cool,
	parse_json($1):elite, 
	parse_json($1):fans,
	parse_json($1):friends, 
	parse_json($1):funny,
	parse_json($1):name, 
	parse_json($1):review_count, 
	parse_json($1):useful,
	parse_json($1):user_id,
	parse_json($1):yelping_since
	FROM udacityproject.STAGING.yelp_user;

--6 yelp_review

INSERT INTO review(business_id, cool, date, funny, review_id, stars, text, useful, user_id) 
SELECT parse_json($1):business_id, 
	parse_json($1):cool, 
	parse_json($1):date::date, 
	parse_json($1):funny, 
	parse_json($1):review_id, 
	parse_json($1):stars, 
	parse_json($1):text, 
	parse_json($1):useful, 
	parse_json($1):user_id 
FROM udacityproject.STAGING.yelp_review;
--7 yelp_checkin

INSERT INTO checkin(business_id, date)
SELECT parse_json($1):business_id,
		parse_json($1):date 
		FROM udacityproject.staging.yelp_checkin;

--8 covid

INSERT INTO covid(Call_To_Action_enabled,Covid_Banner,Grubhub_enabled,Request_a_Quote_Enabled, Temporary_Closed_Until,
Virtual_Services_Offered,business_id,delivery_or_takeout,highlights)
SELECT 
	parse_json($1):"Call To Action enabled",
	parse_json($1):"Covid Banner",
	parse_json($1):"Grubhub enabled",
	parse_json($1):"Request a Quote Enabled",
	parse_json($1):"Temporary Closed Until",
	parse_json($1):"Virtual Services Offered",
	parse_json($1):"business_id",
	parse_json($1):"delivery or takeout",
	parse_json($1):"highlights" 
from udacityproject.staging.yelp_covid_features ;
