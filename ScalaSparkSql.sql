SELECT submission_date, state, total_cases, new_case, total_deaths, new_death, covid_cases_rate, covid_deaths_rate,TO_DATE(submission_date, 'MM/DD/YYYY') t
FROM public.casestudy_spark


--Total cases and deaths in USA
SELECT 'Total cases and deaths in USA' Note ,sum(total_cases) TotalCases, sum(total_deaths) TotalDeaths
FROM public.casestudy_spark;

--Top 5 states with high COVID Cases
select  * from
(select state, sum(total_cases) TotalCases
FROM public.casestudy_spark
group by state) a
order by TotalCases desc
limit 5

--Total cases and death in California as of 5/20/2021
SELECT 'Total cases and deaths in California' Note ,sum(total_cases) TotalCases, sum(total_deaths) TotalDeaths
FROM public.casestudy_spark
where state = 'CA'
and TO_DATE(submission_date, 'MM/DD/YYYY') <= '5/20/2021'

--When percent increase of Covid cases is more than 20% compared to previous submission in California
WITH tab AS (
SELECT submission_date , state, total_cases, new_case, (new_case/total_cases::float)*100 PercentNewCases
FROM public.casestudy_spark
where state = 'CA' and total_cases > 0 
ORDER BY TO_DATE(submission_date, 'MM/DD/YYYY') asc
) ,
tab2 as (
SELECT
	submission_date, 
	PercentNewCases,
	LAG(PercentNewCases,1) OVER (
		ORDER BY submission_date
	) PrevPercentNewCases
	,PercentNewCases - 	LAG(PercentNewCases,1) OVER (
		ORDER BY submission_date
	) diff
FROM
	tab
)
select * from tab2 where diff >= 25
--5.	Which is safest state comparatively
with tab as(
SELECT state ,sum(new_case) totalcases, sum(new_death) totaldeaths
FROM public.casestudy_spark
group by state),
tab2 as(
select state, (totaldeaths/totalcases::float)*100 deathrate  from tab 
where totalcases > 0
)
select * from tab2
order by deathrate 
