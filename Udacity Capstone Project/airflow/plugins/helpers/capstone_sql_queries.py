class DataChecks:
    immigration = ("""
        SELECT DISTINCT COUNT(*)        
        FROM immigration
        WHERE (immig_id IS NULL OR
        origin_country IS NULL OR
        city IS NULL OR
        state_cd IS NULL OR
        arrival_date IS NULL OR
        age IS NULL OR
        gender IS NULL OR
        visa_type IS NULL)
        """)
        
        
    demographics = ("""
        SELECT DISTINCT COUNT(*)        
        FROM demographics
        WHERE (city IS NULL OR
        state IS NULL OR
		median_age IS NULL OR
		male_pop IS NULL OR
		female_pop IS NULL OR
		total_pop IS NULL OR
		num_veterans IS NULL OR
		foreign_born IS NULL OR
		avg_household_size IS NULL OR
		num_american_indian IS NULL OR
		num_asian IS NULL OR
		num_black IS NULL OR
		num_hispanic IS NULL OR
		num_white IS NULL)
        """)
    
    temperature = ("""
        SELECT DISTINCT COUNT(*)
        FROM temperature
        WHERE (temp_id IS NULL OR
        city IS NULL OR
        avg_temp IS NULL OR
        year IS NULL OR
        month IS NULL)
        """)
    
    airport = ("""
        SELECT DISTINCT COUNT(*)
        FROM airport
        WHERE (airport_id IS NULL OR
        city IS NULL OR
        state_cd IS NULL OR
        airport_name IS NULL OR
        airport_type IS NULL OR
        iata_cd IS NULL)
        """)
    
    region = ("""
        SELECT DISTINCT COUNT(*)
        FROM region
        WHERE (city_id IS NULL OR
        city IS NULL OR
        state IS NULL OR
        state_cd IS NULL OR
        county IS NULL OR
        country IS NULL OR
        latitude IS NULL OR
        longitude IS NULL)
        """)

       

        
        