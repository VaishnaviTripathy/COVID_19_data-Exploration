--Data Exploration of covid-19 around the Globe from 28-jan-2020 to 15-jan2022 using SQL IN BigQuery 
 
SELECT
  *
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`; 
  
# Selecting the required data for exploration

SELECT
  location,
  date,
  total_cases,
  new_cases,
  total_deaths,
  population
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`
WHERE
  continent IS NOT NULL
ORDER BY
  location,
  date; 
  
 #looking AT total_deaths VS total_cases

SELECT
  location,
  date,
  total_cases,
  total_deaths,
  (total_deaths/total_cases)*100 AS death_percentage_eachday
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`
WHERE
  continent IS NOT NULL
ORDER BY
  location,
  date; 
  
  #looking FOR the death percentage OF locations starting with the word united

SELECT
  location,
  date,
  total_cases,
  total_deaths,
  (total_deaths/total_cases)*100 AS death_percentage_eachday
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`
WHERE
  location LIKE 'United%'
ORDER BY
  date; 
  
  #looking FOR the total number of deaths at each location per month per year
 
SELECT
  location,
  EXTRACT(MONTH
  FROM
    date) AS month_of_date,
  EXTRACT(year
  FROM
    date) AS Year_of_date,
  SUM(new_cases)AS total_cases_per_month,
  SUM(new_deaths)AS total_deaths_per_month,
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`
WHERE
  continent IS NOT NULL
GROUP BY
  location,
  month_of_date,
  Year_of_date
ORDER BY
  total_deaths_per_month DESC; 
  
  # looking the total number OF deaths IN the last two year vs total cases recorded IN 2020 AND 2021 
  
SELECT
  location,
  SUM(new_deaths)AS total_deaths_20_21,
  SUM(new_cases)AS total_cases_20_21,
  (SUM(new_deaths)/ SUM(new_cases))*100 AS death_percentage
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`
WHERE
  EXTRACT (year FROM date) IN (2021, 2020)
  AND continent IS NOT NULL
GROUP BY
  location
ORDER BY
  total_deaths_20_21 DESC; 
  
   #looking FOR total cases VS total population 
 
SELECT
  location,
  date,
  population,
  total_cases,
  (total_cases/population)*100 AS percentage_covid_positive
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`
WHERE
  continent IS NOT NULL
ORDER BY
  percentage_covid_positive DESC;
  
  # Highestinfectious rate compared TO population # united states has the highest death count

SELECT
  location,
  population,
  MAX(total_cases) AS highestinfections,
  MAX((total_cases/population))*100 AS percentpopulationinfected
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`
WHERE
  continent IS NOT NULL
GROUP BY
  location,
  population
ORDER BY
  percentpopulationinfected DESC ; 
  
  #Showing countries with highest death count per population

SELECT
  location,
  population,
  SUM(new_deaths) AS total_death_count,
  MAX(total_deaths/(population))*100 AS death_percentage_per_population
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`
WHERE
  continent IS NOT NULL
GROUP BY
  location,
  population
ORDER BY
  SUM(new_deaths) DESC;
  
  # Total death counts per continent 
SELECT
  continent,
  SUM(new_deaths) AS total_death_count
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`
WHERE
  continent IS NOT NULL
GROUP BY
  continent
ORDER BY
  SUM(new_deaths) DESC; 
 
 ##global numbers

SELECT
  date,
  SUM(new_cases)AS total_cases_count,
  SUM(new_deaths) AS total_death_count,
  (SUM(new_deaths)/SUM(new_cases))*100 AS death_rate
FROM
  `vertical-tuner-330611.covid_19.covid_deaths`
WHERE
  continent IS NOT NULL
GROUP BY
  date
ORDER BY
  date,
  SUM(new_cases),
  SUM(new_deaths); 
  
  #Looking AT total population VS vaccination
SELECT
  death.continent,
  death.location,
  death.date,
  death.population,
  vac.new_vaccinations,
  SUM(new_vaccinations) OVER (PARTITION BY death.location ORDER BY death.location, death.date)AS rolling_count_vaccination
FROM
  `vertical-tuner-330611.covid_19.covid_deaths` AS death
JOIN
  `vertical-tuner-330611.covid_19.covid_vaccination` AS vac
ON
  death.location = vac.location
  AND death.date=vac.date
WHERE
  death.continent IS NOT NULL
ORDER BY
    2,3; 
    
    #USE CTE
WITH
  popvsvac AS (
  SELECT
    death.continent,
    death.location,
    death.date,
    death.population,
    vac.new_vaccinations,
    SUM(new_vaccinations) OVER (PARTITION BY death.location ORDER BY death.location, death.date)AS rolling_count_vaccination
  FROM
    `vertical-tuner-330611.covid_19.covid_deaths` AS death
  JOIN
    `vertical-tuner-330611.covid_19.covid_vaccination` AS vac
  ON
    death.location = vac.location
    AND death.date=vac.date
  WHERE
    death.continent IS NOT NULL )
SELECT
  *,
  (rolling_count_vaccination/population)
FROM
  popvsvac
ORDER BY
  location,
  date;
