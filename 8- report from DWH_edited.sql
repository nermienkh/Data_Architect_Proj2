/*report business name, temperature, precipitation, ratings*/

SELECT  b.name, p.min_t, p.max_t, p.normal_min, p.normal_max, p.precipitation, p.precipitation_normal,r.stars
FROM business AS b
JOIN fact_table AS f 
ON b.business_id = f.business_id_f
JOIN precipitation_temperature AS p
ON p.date_t = f.date_precipitation_temperature
JOIN review AS r
ON r.review_id = f.review_id;