
-- covid_19_dw.country

\! echo "Carregando dados na tabela country..."

INSERT INTO covid_19_dw.country(country)
SELECT distinct country	FROM covid_19.coronavirus_who_stg
EXCEPT
SELECT country FROM covid_19_dw.country
ORDER BY 1;

VACUUM ANALYZE covid_19_dw.country;

----------------------------------------------------------------------------

-- covid_19_dw.world_data

\! echo "Atualizando dados da China na tabela coronavirus_who..."

UPDATE covid_19.coronavirus_who_stg
    SET total_cases = w.total_cases, total_deaths = w.total_deaths
    FROM covid_19.world_in_data w
    WHERE country = location AND coronavirus_who_stg.date = w.date AND w.location = 'China';

\! echo "Carregando dados na tabela fato world_data..."

INSERT INTO covid_19_dw.world_data
SELECT d.date, c.country_id, f.total_cases, f.total_deaths, f.total_recovered, f.critical, f.total_tests
	FROM covid_19.coronavirus_who_stg f
	JOIN covid_19_dw.date d ON d.date=f.date
	JOIN covid_19_dw.country c ON c.country=f.country;

VACUUM ANALYZE covid_19_dw.world_data;

----------------------------------------------------------------------------

-- covid_19_dw.italy

\! echo "Carregando dados na tabela fato italy..."

INSERT INTO covid_19_dw.italy
SELECT d.date, r.region_id, total_cases
	FROM covid_19.italy_stg f
	JOIN covid_19_dw.date d ON f.date=d.date
	JOIN covid_19_dw.region_italy r ON f.region=r.region;

VACUUM ANALYZE covid_19_dw.italy;

----------------------------------------------------------------------------

-- covid_19_dw.brazil

\! echo "Carregando dados na tabela fato brazil..."

INSERT INTO covid_19_dw.brazil
SELECT d.date, s.state_id, confirmados, obitos
	FROM covid_19.brazil_stg f
	JOIN covid_19_dw.date d ON f.data=d.date
	JOIN covid_19_dw.state s ON f.estado=s.state_cd AND s.country = 'Brazil';

VACUUM ANALYZE covid_19_dw.brazil;

----------------------------------------------------------------------------

-- covid_19_dw.usa

\! echo "Carregando dados na tabela fato usa..."

INSERT INTO covid_19_dw.usa
SELECT d.date, s.state_id, positive, negative, hospitalized, death
	FROM covid_19.usa_stg f
	JOIN covid_19_dw.date d ON f.date=d.date
	JOIN covid_19_dw.state s ON f.state=s.state_cd AND s.country = 'United States';

VACUUM ANALYZE covid_19_dw.usa;

----------------------------------------------------------------------------

-- covid_19_dw.spain

\! echo "Carregando dados na tabela fato spain..."

INSERT INTO covid_19_dw.spain
SELECT d.date, s.state_id, cases, hospitalized, icu, death, recovered
	FROM covid_19.spain_stg f
	JOIN covid_19_dw.date d ON f.date=d.date
	JOIN covid_19_dw.state s ON f.state=s.state_cd AND s.country = 'Spain';

VACUUM ANALYZE covid_19_dw.spain;

----------------------------------------------------------------------------

-- covid_19_dw.tests

\! echo "Carregando dados na tabela fato tests..."

INSERT INTO covid_19_dw.tests
SELECT d.date, c.country_id, tests
	FROM covid_19.tests_stg f
	JOIN covid_19_dw.date d ON d.date=f.date
	JOIN covid_19_dw.country c ON c.country=f.country
	WHERE (d.date, c.country_id) 
	NOT IN (SELECT date, country_id FROM covid_19_dw.tests);

VACUUM ANALYZE covid_19_dw.tests;