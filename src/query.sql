select * from
(
select *,
	year(Date),
	month(date)
from 
	oag_flight_international
)t1
​
join
​
(select 
	country_id,
	ceiling(avg(number_of_cases)) as avg_covid_cases,
	month(Date), year(Date)
from 
	covid_case_country
group by 
	month(Date),
	year(Date), 
	country_id
) t2
​
on 
	t1.country_id = t2.country_id
and
	t1.month(date) = t2.month(Date)
​
join
	Country c
​
on
	t1.country_id = c.country_id 

-- country/covid
SELECT *
FROM country AS c
JOIN covid_case_country AS cc
ON c.`Country Id` = cc.`Country Id`
JOIN covid_death_country AS dc
ON cc.`Country Id` = dc.`Country Id` AND cc.Date = dc.Date
JOIN vaccine_country AS vc
ON cc.`Country Id` = vc.`Country Id` AND cc.Date = vc.Date
​
-- country/travel
SELECT *
FROM country AS c
JOIN oag_flight_international AS ofi
ON c.`Country Id` = ofi.`Country Id`
JOIN oag_seat_international AS osi
ON ofi.`Country Id` = osi.`Country Id` AND ofi.Date = osi.Date
​
-- state/covid
SELECT *
FROM us_state AS s
JOIN covid_case_state AS cs
ON s.`US State Id` = cs.`State Id`
JOIN covid_death_state AS ds
ON cs.`State Id` = dc.`State Id` AND cs.Date = ds.Date
JOIN vaccine_country AS vs
ON cs.`State Id` = vc.`State Id` AND cs.Date = vs.Date
​
-- state/travel
SELECT *
FROM opensky_flight_state as ofs
JOIN us_state AS s
ON ofs.`State Id` = s.`Us State Id`

SELECT 
country_name, number_of_cases, number_of_deaths, number_people_vaccinated, percentage_people_vaccinated, cc.Date
FROM covid_case_country AS cc
JOIN country c 
ON cc.country_id = c.country_id
RIGHT JOIN vaccine_country vc 
ON c.country_id = vc.country_id AND vc.Date = cc.Date
JOIN covid_death_country cdc 
ON c.country_id = cdc.country_id AND  cdc.Date = cc.Date
