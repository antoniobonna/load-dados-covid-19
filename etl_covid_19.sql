
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
SELECT d.date, r.region_id, icu, hospitalized, home_isolation, recovered, deaths, cases, tests
	FROM covid_19.italy_stg f
	JOIN covid_19_dw.date d ON f.date=d.date
	JOIN covid_19_dw.region_italy r ON f.region_cd=r.region_id;

VACUUM ANALYZE covid_19_dw.italy;

----------------------------------------------------------------------------

-- covid_19_dw.brazil

\! echo "Carregando dados na tabela fato brazil..."

INSERT INTO covid_19_dw.brazil
SELECT d.date, s.state_id, confirmados, obitos, recovered, suspects, tests
	FROM covid_19.brazil_stg f
	JOIN covid_19_dw.date d ON f.data=d.date
	JOIN covid_19_dw.state s ON f.estado=s.state_cd AND s.country = 'Brazil';

VACUUM ANALYZE covid_19_dw.brazil;

----------------------------------------------------------------------------

-- covid_19_dw.usa

\! echo "Carregando dados na tabela fato usa..."

INSERT INTO covid_19_dw.usa
SELECT d.date, s.state_id, positive, negative, pending, hospitalized, death
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
	WHERE (d.date, c.country_id) NOT IN (SELECT date, country_id FROM covid_19_dw.tests);

VACUUM ANALYZE covid_19_dw.tests;

----------------------------------------------------------------------------

-- covid_19_dw.healthcare_patients

\! echo "Carregando dados na tabela fato healthcare_patients..."

INSERT INTO covid_19_dw.healthcare_patients
SELECT d.date, h.healthcare_id, p.patient_id, patients
	FROM covid_19.vw_healthcare_patient f
	JOIN covid_19_dw.date d ON f.date=d.date
	JOIN covid_19_dw.healthcare h ON f.healthcare=h.healthcare
	JOIN covid_19_dw.patient p ON f.patient_classification=p.patient 
	WHERE not patients is null
	AND (d.date, h.healthcare_id, p.patient_id) NOT IN (SELECT date, healthcare_id, patient_id FROM covid_19_dw.healthcare_patients);

VACUUM ANALYZE covid_19_dw.healthcare_patients;

----------------------------------------------------------------------------

-- covid_19_dw.healthcare_inpatients

\! echo "Carregando dados na tabela fato healthcare_inpatients..."

INSERT INTO covid_19_dw.healthcare_inpatients
SELECT d.date, h.healthcare_id, p.patient_id, b.bed_id, inpatients
	FROM covid_19.vw_healthcare_bed f
	JOIN covid_19_dw.date d ON f.date=d.date
	JOIN covid_19_dw.healthcare h ON f.healthcare=h.healthcare
	JOIN covid_19_dw.patient p ON f.patient_classification=p.patient 
	JOIN covid_19_dw.bed b ON f.beds=b.bed
	WHERE not inpatients is null
	AND (d.date, h.healthcare_id, p.patient_id, b.bed_id) NOT IN (SELECT date, healthcare_id, patient_id, bed_id FROM covid_19_dw.healthcare_inpatients);

VACUUM ANALYZE covid_19_dw.healthcare_inpatients;

----------------------------------------------------------------------------

-- covid_19.city_population

\! echo "Carregando dados na tabela city_population..."

INSERT INTO covid_19.city_population
SELECT DISTINCT ibge_cd,city,state_cd,'Brazil',population
	FROM covid_19.brazil_cities_stg
	WHERE ibge_cd not in (SELECT city_cd FROM covid_19.city_population WHERE country = 'Brazil');

VACUUM ANALYZE covid_19.city_population;

----------------------------------------------------------------------------

-- covid_19_dw.city (Brazil)

\! echo "Carregando dados na tabela city..."

INSERT INTO covid_19_dw.city (city_cd,city,state_cd,country)
SELECT DISTINCT ibge_cd,city,state_cd,'Brazil'
	FROM covid_19.brazil_cities_stg
	WHERE ibge_cd not in (SELECT city_cd FROM covid_19_dw.city WHERE country = 'Brazil');

VACUUM ANALYZE covid_19_dw.city;

----------------------------------------------------------------------------

-- covid_19_dw.brazil_cities

\! echo "Carregando dados na tabela fato brazil_cities..."

COPY (
	SELECT d.date,c.city_id,cases,deaths
		FROM covid_19.brazil_cities_stg f
		JOIN covid_19_dw.date d ON f.date=d.date
		JOIN covid_19_dw.city c ON f.ibge_cd=c.city_cd AND c.country = 'Brazil'
		WHERE (d.date,c.city_id) NOT IN (SELECT date,city_id FROM covid_19_dw.brazil_cities)
) to '/home/ubuntu/dump/dados_covid_19/cities.txt';
COPY covid_19_dw.brazil_cities FROM '/home/ubuntu/dump/dados_covid_19/cities.txt';

VACUUM ANALYZE covid_19_dw.brazil_cities;

----------------------------------------------------------------------------

-- covid_19_dw.city (United States)

\! echo "Carregando dados na tabela city..."

INSERT INTO covid_19_dw.city (city_cd,city,state_cd,country)
SELECT DISTINCT city_cd,city,s.state_cd,'United States'
	FROM covid_19.usa_cities_stg f
	JOIN covid_19_dw.state s ON f.state=s.state
	WHERE city_cd not in (SELECT city_cd FROM covid_19_dw.city WHERE country = 'United States');

VACUUM ANALYZE covid_19_dw.city;

----------------------------------------------------------------------------

-- covid_19_dw.usa_cities

\! echo "Carregando dados na tabela fato usa_cities..."

COPY (
	SELECT d.date, c.city_id, cases, deaths
		FROM covid_19.usa_cities_stg f
		JOIN covid_19_dw.date d ON f.date=d.date
		JOIN covid_19_dw.city c ON c.city_cd=f.city_cd AND c.country = 'United States'
) to '/home/ubuntu/dump/dados_covid_19/cities.txt';
COPY covid_19_dw.usa_cities FROM '/home/ubuntu/dump/dados_covid_19/cities.txt';

VACUUM ANALYZE covid_19_dw.usa_cities;

----------------------------------------------------------------------------

-- covid_19_dw.hospitalization

\! echo "Carregando dados na tabela fato hospitalization..."

INSERT INTO covid_19_dw.hospitalization
SELECT d.date, l.local_id, b.bed_id, hospitalized
	FROM covid_19.vw_local_hospitalization f
	JOIN covid_19_dw.date d ON f.date=d.date
	JOIN covid_19_dw.local l ON f.local=l.local
	JOIN covid_19_dw.bed b ON f.bed=b.bed
	WHERE not hospitalized is null 
	AND (d.date, l.local_id, b.bed_id) NOT IN (SELECT date, local_id, bed_id FROM covid_19_dw.hospitalization);

VACUUM ANALYZE covid_19_dw.hospitalization;