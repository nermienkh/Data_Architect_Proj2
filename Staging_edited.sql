-- use WAREHOUSE compute_wh; create internal warehouse then select it if not selected

-- create file formats
1- create or replace file format mycsvformat type='CSV' compression='auto' field_delimiter=',' 
record_delimiter = '\n'  skip_header=1 error_on_column_count_mismatch=true null_if = ('NULL', 'null') empty_field_as_null = true;

2- create or replace file format my_json_format type = 'json' strip_outer_array = true;


-- staging files
-- 1- climate_temprature file 
1- create table climate_temprature  (date string, min int, max int, normal_min double,
                                                 normal_max double); 

2- create or replace stage climate_temperature file_format = mycsvformat;
3- put file:///D:/Udacity_Data_Architect/Project2/temperature.csv @climate_temperature auto_compress=true parallel=4;


4- copy into climate_temprature from @climate_temperature/temperature.csv.gz file_format=mycsvformat on_error='skip_file';

-- 2- climate_percipartion file 

1- create table climate_Precipitation (date string, precipitation string, precipitation_normal
 double);

2- create or replace stage climate_Precipitation file_format = mycsvformat;
3- put file:///D:/Udacity_Data_Architect/Project2/Precipitation.csv @climate_Precipitation auto_compress=true parallel=4;

-- use WAREHOUSE compute_wh; create internal warehouse then select it if not selected
4- copy into climate_Precipitation from @climate_Precipitation/Precipitation.csv.gz
 file_format=mycsvformat on_error='skip_file';


-- 3-YELP business 
-- i will be using variant datatype 

1- create table  Yelp_business (business_json_data variant);
2- create or replace stage Yelp_business file_format = my_json_format;
3- put file:///D:/Udacity_Data_Architect/Project2/YELP/yelp_academic_dataset_business.json @Yelp_business auto_compress=true parallel=4;

4- copy into Yelp_business  from @Yelp_business/yelp_academic_dataset_business.json.gz 
 file_format=my_json_format on_error='skip_file';

-- and so on for the rest of yelp files you just need to change table name and file path

-- 4-Yelp_checkin 
1- create table  Yelp_checkin (checkin_json_data variant);
2- create or replace stage Yelp_checkin file_format = my_json_format;
3- put file:///D:/Udacity_Data_Architect/Project2/YELP/yelp_academic_dataset_checkin.json
@Yelp_checkin auto_compress=true parallel=4;
4- copy into Yelp_checkin  from @Yelp_checkin/yelp_academic_dataset_checkin.json.gz 
file_format=my_json_format on_error='skip_file';

--5- Yelp_review
1- create table  Yelp_review (review_json_data variant);
2- create or replace stage Yelp_review file_format = my_json_format;
3- put file:///D:/Udacity_Data_Architect/Project2/YELP/yelp_academic_dataset_review.json @Yelp_review auto_compress=true parallel=4;
4- copy into Yelp_review  from @Yelp_review/yelp_academic_dataset_review.json.gz 
file_format=my_json_format on_error='skip_file';
 
-- 6-  Yelp_tip
1- create table  Yelp_tip (tip_json_data variant);
2- create or replace stage Yelp_tip file_format = my_json_format;
3- put file:///D:/Udacity_Data_Architect/Project2/YELP/yelp_academic_dataset_tip.json @Yelp_tip auto_compress=true parallel=4;
4- copy into Yelp_tip  from @Yelp_tip/yelp_academic_dataset_tip.json.gz 
file_format=my_json_format on_error='skip_file';


-- 7-  Yelp_user
1- create table  Yelp_user (user_json_data variant);
2- create or replace stage Yelp_user file_format = my_json_format;
3- put file:///D:/Udacity_Data_Architect/Project2/YELP/yelp_academic_dataset_user.json @Yelp_user auto_compress=true parallel=4;
4- copy into Yelp_user  from @Yelp_user/yelp_academic_dataset_user.json.gz 
file_format=my_json_format on_error='skip_file';
 
-- 8-  yelp_covid_features
1- create table  yelp_covid_features (covid_features_json_data variant);
2- create or replace stage yelp_covid_features file_format = my_json_format;
3- put file:///D:/Udacity_Data_Architect/Project2/YELP/yelp_covid_features.json @yelp_covid_features auto_compress=true parallel=4;
4- copy into yelp_covid_features  from @yelp_covid_features/yelp_covid_features.json.gz 
file_format=my_json_format on_error='skip_file';








