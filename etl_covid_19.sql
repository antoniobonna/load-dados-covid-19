
-- covid_19_dw.country

\! echo "Carregando dados na tabela date..."

INSERT INTO covid_19_dw.country(country)
SELECT distinct country	FROM covid_19.coronavirus_who_stg
EXCEPT
SELECT country FROM covid_19_dw.country
ORDER BY 1;

VACUUM ANALYZE covid_19_dw.country;

----------------------------------------------------------------------------

-- covid_19_dw.world_data

\! echo "Carregando dados na tabela fato world_data..."

INSERT INTO covid_19_dw.world_data
SELECT d.date, c.country_id, f.total_cases, f.total_deaths, f.total_recovered, f.critical, f.total_tests
	FROM covid_19.coronavirus_who_stg f
	JOIN covid_19_dw.date d ON d.date=f.date
	JOIN covid_19_dw.country c ON c.country=f.country

VACUUM ANALYZE covid_19_dw.world_data;

----------------------------------------------------------------------------

-- covid_19_dw.web_engagement_daily

\! echo "Carregando dados na tabela fato web_engagement_daily..."

COPY(
SELECT d.date, co.company_id, c.country_id, s.source_id, f.visits, f.avg_visit_duration, f.pages_visit::real, f.bounce_rate::real, 
	f.total_page_views::real, w.desktop_share::real
	FROM similar_web.web_traffic_engagement_source_daily_stg f
	JOIN similar_web.web_traffic_engagement_daily_stg w ON f.date=w.date AND f.country=w.country AND f.name=w.name
	JOIN covid_19_dw.date d ON d.date=f.date
	JOIN covid_19_dw.company co ON co.company=f.name
	JOIN covid_19_dw.country c ON c.country=f.country
	JOIN covid_19_dw.source s ON s.source=f.source
EXCEPT
SELECT date, company_id, country_id, source_id, visits, avg_visit_duration, pages_visit, bounce_rate, total_page_views, desktop_share
	FROM covid_19_dw.web_engagement_daily WHERE date >= current_date - 31 
) to '/home/ubuntu/dump/web_engagement_daily.txt';
COPY covid_19_dw.web_engagement_daily FROM '/home/ubuntu/dump/web_engagement_daily.txt';

VACUUM ANALYZE covid_19_dw.web_engagement_daily;

----------------------------------------------------------------------------

-- covid_19_dw.web_marketing_desktop_daily

\! echo "Carregando dados na tabela fato web_marketing_desktop_daily..."

COPY(
SELECT d.date, co.company_id, c.country_id, ch.channel_id, channel_traffic, avg_visit_duration, pages_visit::real, bounce_rate::real
	FROM similar_web.web_marketing_desktop_daily_stg f
	JOIN covid_19_dw.date d ON d.date=f.time_period
	JOIN covid_19_dw.company co ON co.company=f.name
	JOIN covid_19_dw.country c ON c.country=f.country
	JOIN covid_19_dw.channel ch ON ch.channel=f.channel
EXCEPT
SELECT date, company_id, country_id, channel_id, channel_traffic, avg_visit_duration, pages_visit, bounce_rate
	FROM covid_19_dw.web_marketing_desktop_daily WHERE date >= current_date - 31 
) to '/home/ubuntu/dump/web_marketing_desktop_daily.txt';
COPY covid_19_dw.web_marketing_desktop_daily FROM '/home/ubuntu/dump/web_marketing_desktop_daily.txt';

VACUUM ANALYZE covid_19_dw.web_marketing_desktop_daily;