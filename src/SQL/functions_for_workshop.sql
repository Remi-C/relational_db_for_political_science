-------------------------------------------------------------------------------
-- functions for workshop
-------------------------------------------------------------------------------



-- query to get the 10 biggest lobbying clients
WITH biggest_lobbying_firms as (
	SELECT "_client_uuid", sum(amount) as s
	FROM consolidated_layer_reports.reports as r
	GROUP BY "_client_uuid"
	ORDER BY s DESC
	LIMIT 10
)
	SELECT f.*, c.client_full_name, c.client_ppb_country
	FROM biggest_lobbying_firms as f
		LEFT OUTER JOIN consolidated_layer_reports.clients as c 
			USING (_client_uuid); 
		

SELECt *
FROM consolidated_layer_reports.clients as c 
WHERE c.client_full_name ILIKE '%Microsoft%'; 



SELECt *
FROM consolidated_layer_bills.committees

-- function to get everything we have about a company
	
	-- given a company name, look for it in the gvkey table 
		DROP FUNCTION IF EXISTS api.top_gvkey_by_fuzzy_name(_approxClientName text) ; 
		CREATE OR REPLACE FUNCTION api.top_gvkey_by_fuzzy_name(_approxClientName text) 
		RETURNS TABLE(gvkey text, confidence float) AS $$ 
		    SELECT c.gvkey, (round((1.1*wsim + sim)::numeric/2.1,3))::float  as confidence
		     FROM lobbying_clients__infogroup_companies.generate_noise_words_permutation(_approxClientName) as c_name_variations
		     	, consolidated_layer_compustat.companies___latest as c 
		        , similarity(c_name_variations, c.company_name_long) as sim
		        , word_similarity(c_name_variations, c.company_name_long) as wsim
		     WHERE c.company_name_long % c_name_variations
		     ORDER BY 1.1*wsim + sim DESC
		     LIMIT 1; 
		$$ LANGUAGE SQL STABLE CALLED ON NULL INPUT COST 10000 PARALLEL SAFE ROWS 1 SET pg_trgm.similarity_threshold TO 0.4 ;
		 	 	
		SELECT r.*, c.company_name_long, c.naics, c.sic4
		FROM api.top_gvkey_by_fuzzy_name('microso') as r 
		LEFT OUTER JOIN consolidated_layer_compustat.companies___latest as c USING (gvkey);
	
	
	
	
	-- given a company name, look for it in the infogroup table 
		DROP FUNCTION IF EXISTS api.top_infogroupid_by_fuzzy_name(_approxClientName text, _company_state VARCHAR(2)) ; 
		CREATE OR REPLACE FUNCTION api.top_infogroupid_by_fuzzy_name(_approxClientName text, _company_state VARCHAR(2) DEFAULT NULL) 
		  RETURNS  TABLE (  
         	infogroup_id varchar(9) 
            , sim_name real
            , is_identical_state boolean
            , company_holding_status boolean
            , employee_size_location int
            ,  sales_volume_location int
    	)  AS $$ 
		    SELECT r.infogroup_id, r.sim_name, r.is_identical_state , r.company_holding_status, r.employee_size_location, r.sales_volume_location
		    FROM consolidated_layer_infogroup.find_top5_matching_companies_with_alias(_approxClientName, _company_state) WITH ordinality as r
		     ORDER BY r.ordinality ASC
		     LIMIT 1; 
		$$ LANGUAGE SQL STABLE CALLED ON NULL INPUT COST 10000 PARALLEL SAFE ROWS 1 SET pg_trgm.similarity_threshold TO 0.7 ;
		 	 	
		SELECT r.infogroup_id, r.sim_name, c.company_name, cr.primary_naics_code, cr.primary_sic_code, c.parent_number, c.subsidiary_number, cr.sales_volume_location, cr.employee_size_location
		FROM api.top_infogroupid_by_fuzzy_name('Microsoft cor', 'WA') as r 
			INNER JOIN consolidated_layer_infogroup.companies as c USING (infogroup_id)
			LEFT OUTER JOIN relational_layer_infogroup.companies as cr USING (infogroup_id);
	
	
	
	-- givne a company name, look for industry codes :
	DROP FUNCTION IF EXISTS api.top_client_industry_by_fuzzy_name(_approxClientName text) ; 
		CREATE OR REPLACE FUNCTION api.top_client_industry_by_fuzzy_name(_approxClientName text) 
		RETURNS TABLE(_client_uuid uuid, confidence float) AS $$ 
		    SELECT c."_client_uuid", (round((1.1*wsim + sim)::numeric/2.1,3))::float  as confidence
		     FROM lobbying_clients__infogroup_companies.generate_noise_words_permutation(_approxClientName) as c_name_variations
		     	, lobbying_clients__industry_codes.clients__industry_codes as c 
		        , similarity(c_name_variations, c.company_reference_name) as sim
		        , word_similarity(c_name_variations, c.company_reference_name) as wsim
		     WHERE c.company_reference_name % c_name_variations
		     ORDER BY 1.1*wsim + sim DESC
		     LIMIT 1; 
		$$ LANGUAGE SQL STABLE CALLED ON NULL INPUT COST 10000 PARALLEL SAFE ROWS 1 SET pg_trgm.similarity_threshold TO 0.4;
		 	 	
		SELECT r.*, c.company_reference_name, c.primary_naics, c.primary_sic4
		FROM api.top_client_industry_by_fuzzy_name('microsoft') as r 
		LEFT OUTER JOIN lobbying_clients__industry_codes.clients__industry_codes as c  USING (_client_uuid);

	 