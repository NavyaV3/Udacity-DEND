CREATE TABLE IF NOT EXISTS public.immigration (
	immig_id FLOAT8 NOT NULL,
	origin_country varchar(256),
	city varchar(256) NOT NULL,
	state_cd varchar(3) NOT NULL,
	arrival_date DATE,
	age INT,
	gender CHAR,
	visa_type varchar(4),
	CONSTRAINT immigration_pkey PRIMARY KEY(immig_id)
);	

CREATE TABLE IF NOT EXISTS public.demographics (
	city varchar(256) NOT NULL
	,state varchar(100) NOT NULL
	,median_age FLOAT8 NOT NULL
	,male_pop INT NOT NULL
	,female_pop INT NOT NULL
	,total_pop INT NOT NULL
	,num_veterans INT NOT NULL
	,foreign_born INT NOT NULL
	,avg_household_size FLOAT8 NOT NULL
	,num_american_indian BIGINT NOT NULL
	,num_asian BIGINT NOT NULL
	,num_black BIGINT NOT NULL
	,num_hispanic BIGINT NOT NULL
	,num_white BIGINT NOT NULL
	,CONSTRAINT demographics_pkey PRIMARY KEY(city)
);

CREATE TABLE IF NOT EXISTS public.temperature (
	temp_id INT NOT NULL
	,city varchar(256) NOT NULL
	,avg_temp FLOAT8 NOT NULL
	,year INT NOT NULL
	,month INT NOT NULL
    ,CONSTRAINT temperature_pkey PRIMARY KEY(temp_id)
	);

CREATE TABLE IF NOT EXISTS public.airport (
	airport_id varchar(10) NOT NULL
	,city varchar(50) NOT NULL
	,state_cd varchar(2) NOT NULL
	,airport_name varchar(250) NOT NULL
	,airport_type varchar(250) NOT NULL
	,iata_cd varchar(5) NOT NULL,
	CONSTRAINT airport_pkey PRIMARY KEY(airport_id)
);

CREATE TABLE IF NOT EXISTS public.region (
	city_id INT NOT NULL
	,city varchar(250) NOT NULL
	,state varchar(50) NOT NULL
	,state_cd varchar(3) NOT NULL
	,county varchar(250) NOT NULL
	,country varchar(50) NOT NULL
	,latitude FLOAT8 NOT NULL
	,longitude FLOAT8 NOT NULL
	,CONSTRAINT region_pkey PRIMARY KEY(city_id)
);
