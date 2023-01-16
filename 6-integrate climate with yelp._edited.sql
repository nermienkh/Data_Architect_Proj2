
-- query to integrate yelp with  climate  data 

SELECT * 
	FROM udacityproject.ods.precipitation AS p
	JOIN udacityproject.ods.review AS r 
	ON r.date = p.date_t
	JOIN udacityproject.ods.temperature AS t
	ON t.date_t = r.date;


-- note we can use business id attribute from the review table to link the rest of the tables