-- DWH - star schema 

-- 1- dimension tables

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

create or replace TABLE REVIEW (
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
	foreign key (BUSINESS_ID) references UDACITYPROJECT.DWH.BUSINESS(BUSINESS_ID),
	foreign key (USER_ID) references UDACITYPROJECT.DWH.USER(USER_ID)
);



INSERT INTO business(business_id,name, address, city, state, postal_code, latitude,longitude,stars, review_count, is_open, attribute, categories, hours)
SELECT * FROM udacityproject.ODS.business;


INSERT INTO user(average_stars,compliment_cool,compliment_cute,compliment_funny,compliment_hot,compliment_list,compliment_more,
compliment_note,compliment_photos,compliment_plain,compliment_profile,compliment_writer,cool,elite, fans,friends, funny,name, review_count, useful,user_id,yelping_since)
SELECT *	FROM udacityproject.ODS.user;


INSERT INTO review(business_id, cool, date, funny, review_id, stars, text, useful, user_id) 
SELECT * FROM udacityproject.ODS.review;




create or replace TABLE UDACITYPROJECT.DWH.PRECIPITATION_Temperature (
	DATE_T DATE,
	PRECIPITATION FLOAT,
	PRECIPITATION_NORMAL FLOAT,
	MIN_T NUMBER(38,0),
	MAX_T NUMBER(38,0),
	NORMAL_MIN FLOAT,
	NORMAL_MAX FLOAT,

	primary key (DATE_T)
);


insert into PRECIPITATION_Temperature  (DATE_T ,PRECIPITATION ,PRECIPITATION_NORMAL ,MIN_T ,MAX_T ,NORMAL_min,NORMAL_MAX )
SELECT p.date_t,p.precipitation,p.precipitation_normal,t.min_t,t.max_t,t.normal_min,t.normal_max
	FROM udacityproject.ods.precipitation AS p
	JOIN udacityproject.ods.temperature AS t
	ON p.date_t = t.date_t;
--------------------------------------------------------------


-- 2- fact table

CREATE TABLE fact_table 
(business_id_f string  foreign key  REFERENCES UDACITYPROJECT.DWH.BUSINESS(BUSINESS_ID), review_id VARIANT foreign key REFERENCES UDACITYPROJECT.DWH.review(review_id), 
user_id_f string foreign key  REFERENCES UDACITYPROJECT.DWH.USER(USER_ID), date_precipitation_temperature date  foreign key  REFERENCES udacityproject.dwh.PRECIPITATION_Temperature(date_t));

-- populate fact table
insert into  fact_table (business_id_f , review_id , user_id_f , date_precipitation_temperature  )
	select b.business_id , r.review_id , u.user_id , t.date_t 
      FROM udacityproject.dwh.PRECIPITATION_Temperature AS  p
	JOIN udacityproject.dwh.review AS r 
	ON r.date = p.date_t
	JOIN udacityproject.dwh.business AS b
	ON b.business_id = r.business_id
   	    	JOIN udacityproject.dwh.user AS u
    	ON u.user_id = r.user_id;


