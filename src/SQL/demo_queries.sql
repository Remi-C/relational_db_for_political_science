------------------------
-- relation DB workshop
--------------- 

-- query to get the 10 biggest lobbying clients
	WITH biggest_lobbying_firms as ( --biggest clients
		SELECT "_client_uuid", sum(amount) as s
		FROM consolidated_layer_reports.reports as s
		GROUP BY "_client_uuid"
		ORDER BY s DESC
		LIMIT 10
	) --  display info about these clients
		SELECT f.*, c.client_full_name, c.client_ppb_country
		FROM biggest_lobbying_firms as f
			LEFT OUTER JOIN consolidated_layer_reports.clients as c 
				USING (_client_uuid); 
			

-- what is the gvkey of a company?
	SELECT r.*, c.company_name_long, c.naics, c.sic4
	FROM api.top_gvkey_by_fuzzy_name('microso') as r 
	LEFT OUTER JOIN consolidated_layer_compustat.companies___latest as c USING (gvkey);


-- what is the infogroup_id of a copmany ( gioves sales, holding, ...)
	SELECT r.*, c.company_name, cr.primary_naics_code, cr.primary_sic_code, c.parent_number, c.subsidiary_number, cr.sales_volume_location, cr.employee_size_location
	FROM api.top_infogroupid_by_fuzzy_name('Microsoft corp', 'WA') as r 
		INNER JOIN consolidated_layer_infogroup.companies as c USING (infogroup_id)
		LEFT OUTER JOIN relational_layer_infogroup.companies as cr USING (infogroup_id);

		
-- What are the industry codes and reference name of a company?
	SELECT r.*, c.company_reference_name, c.primary_naics, c.primary_sic4
	FROM api.top_client_industry_by_fuzzy_name('microso') as r 
	LEFT OUTER JOIN lobbying_clients__industry_codes.clients__industry_codes as c  USING (_client_uuid);
	


--------------------------------
-- issue with datasets 

-- very sparse :
	SELECT "_report_uuid", reporting_year, lobbying_expense, "ACC", "ADV", "AER", "AGR", "ALC", "ANI", "APP", "ART", "AUT", "AVI", "BAN", "BEV", "BNK", "BUD", "CAW", "CDT", "CHM", "CIV", "COM", "CON", "CPI", "CPT", "CSP", "DEF", "DIS", "DOC", "ECN", "EDU", "ENG", "ENV", "FAM", "FIN", "FIR", "FOO", "FOR", "FUE", "GAM", "GOV", "HCR", "HOM", "HOU", "IMM", "IND", "INS", "INT", "LAW", "LBR", "MAN", "MAR", "MED", "MIA", "MMM", "MON", "NAT", "PHA", "POS", "REL", "RES", "RET", "ROD", "RRR", "SCI", "SMB", "SPO", "TAR", "TAX", "TEC", "TOB", "TOR", "TOU", "TRA", "TRD", "TRU", "UNK", "UNM", "URB", "UTI", "VET", "WAS", "WEL"
	FROM _tailored_data_export.reports_with_a_column_per_issue as r
	


SELECT row_nb, std_lname, gender, sw_politician_name, gov_politician_name, icpsr_politician_id, govtrack_id, fec_candidate_id
	, gov_terms, first_yr_in_congress, last_year_in_congress
	, congress_num, congress_begin_yr, congress_end_yr
	, rank_within_party , senior_party_member
	, chamber
	, state_abb, district, party_abb
	, icpsr_com_name, icpsr_com_id 
FROM raw_layer_bills_hc.committee_membership_congress_105_115
ORDER BY sw_politician_name, icpsr_politician_id ; 


SELECT senior_party_member, count(*) as c 
FROM raw_layer_bills_hc.committee_membership_congress_105_115
GROUP BY senior_party_member 
ORDER BY C ASC ;

SELECT *
FROM raw_layer_bills_hc.committee_membership_congress_105_115
WHERE senior_party_member = ANY(ARRAY['24','44','66','62'])


SELECT congress_num, congress_begin_yr, congress_end_yr, count(*) as c 
FROM raw_layer_bills_hc.committee_membership_congress_105_115
GROUP BY congress_num, congress_begin_yr, congress_end_yr
ORDER BY c ASC, congress_num ;

SELECT *
FROM raw_layer_bills_hc.committee_membership_congress_105_115 as com 
WHERE (congress_num = '115' AND congress_begin_yr = '2015')
	OR (congress_num = '109' AND congress_begin_yr = '2011')
	OR (congress_num = '109' AND congress_begin_yr = '2007') ; 


SELECT *
FROM raw_layer_bills_hc.committee_membership_congress_105_115
WHERE chamber = 'NAd'


SELECt *
FROM raw_layer_bills_hc.committee_membership_congress_105_115
WHERE gov_terms ILIKE '%,%'



SELECT state_abb , count(*) as c 
FROM raw_layer_bills_hc.committee_membership_congress_105_115
GROUP BY state_abb


SELECT  icpsr_politician_id, govtrack_id, count(*) over(partition by govtrack_id ) as c2 
FROM raw_layer_bills_hc.committee_membership_congress_105_115 as com
GROUP BY icpsr_politician_id, govtrack_id
ORDER BY c2 DESC;  


SELECt *
FROM raw_layer_bills_hc.committee_membership_congress_105_115 as com
WHERE govtrack_id = '400013' 


SELECT party_abb, count(*) as c
FROM raw_layer_bills_hc.committee_membership_congress_105_115 as com
GROUP BY party_abb
ORDER BY c DESC



SELECt chamber, congress_begin_yr, icpsr_com_id, icpsr_com_name, count(*) OVER(PARTITION BY chamber, congress_begin_yr, icpsr_com_id) as c  
FROM raw_layer_bills_hc.committee_membership_congress_105_115
GROUP BY chamber, congress_begin_yr, icpsr_com_id, icpsr_com_name
ORDER BY c DESC, icpsr_com_id


SELECT *
FROM raw_layer_bills_hc.committee_membership_congress_105_115
WHERE icpsr_com_name = ANY (ARRAY['Majority leader','Minority whip','Speaker' ])


SELECT *
FROM raw_layer_bills_hc.committee_membership_congress_105_115
WHERE icpsr_com_name = ANY (ARRAY['Public works and transportation','Transportation and infrastructure' ])
	AND chamber = 'H' AND congress_begin_yr = '1993' AND icpsr_com_id = '173';

SELECT *
FROM raw_layer_bills_hc.committee_membership_congress_105_115
WHERE icpsr_com_name = ANY (ARRAY['Commerce, science, and transportation','Homeland security and governmental affairs' ])
	AND chamber = 'S' AND congress_begin_yr = '2013' AND icpsr_com_id = '321'
	
	
SELECT icpsr_politician_id, fec_candidate_id, count(*) over(PARTITION BY icpsr_politician_id, fec_candidate_id) AS c 
FROM raw_layer_bills_hc.committee_membership_congress_105_115 as com
--GROUP BY icpsr_politician_id, fec_candidate_id
ORDER BY c DESC ;


SELECT *
FROM raw_layer_bills_hc.committee_membership_congress_105_115 as com
WHERE icpsr_politician_id = '14871' AND fec_candidate_id = 'S6OR00110'


SELECT *
FROM relational_layer_campaign.candidate as c 
WHERE c.candidate_name ILIKE '%Wyden, Ron%'
ORDER BY office, candidate_id

fec_candidate_id 'S6OR00110'



SELECT *
FROM raw_layer_bills_hc.committee_membership_congress_105_115 as com
ORDER BY icpsr_politician_id, row_nb






SELECT *, s.* 
--	max(r[2]::int - r[1]::int)
FROM raw_layer_bills_hc.committee_membership_congress_105_115 as com
	, regexp_split_to_table(gov_terms,'\|') WITH ordinality as s
	, regexp_matches(s,'(\d+)-(\d+)') as r 
--	WHERE r[2]::int - r[1]::int  =60
	WHERE icpsr_politician_id = '2605'
--ORDER BY s.ordinality DESC
	
-- duplicated row?
	-- manufacture an example
	
SELECT icpsr_politician_id, std_lname , com.gov_politician_name , count(*) c
	, row_number() over(PARTITION BY icpsr_politician_id, std_lname) as c2
FROM raw_layer_bills_hc.committee_membership_congress_105_115 as com
GROUP BY icpsr_politician_id, std_lname, com.gov_politician_name 
ORDER BY std_lname, icpsr_politician_id






SELECt row_nb, cm.sw_politician_name, chamber, icpsr_com_id, icpsr_com_name
FROM raw_layer_bills_hc.committee_membership_congress_105_115 as cm
ORDER BY cm.sw_politician_name, chamber, icpsr_com_id, icpsr_com_name, row_nb



SELECt congress_num, congress_num, congress_begin_yr, congress_end_yr, count(*) As c 
FROM raw_layer_bills_hc.committee_membership_congress_105_115
GROUP BY congress_num, congress_begin_yr, congress_end_yr
ORDER BY congress_num, congress_begin_yr, congress_end_yr


SELECt chamber, icpsr_com_id, congress_num, icpsr_com_name
FROM raw_layer_bills_hc.committee_membership_congress_105_115
GROUP BY congress_num , chamber, icpsr_com_id, icpsr_com_name
ORDER BY chamber, icpsr_com_id,congress_num,  icpsr_com_name  

SELECT
    cm.committee_thomas_id,
    c.committee_thomas_id,
    l.bioguide_id,
    l.govtrack_id,
    l.other_ids,
    cm.congress_session,
    cm.member_bioguide_id
FROM
    consolidated_layer_bills.legislators l
INNER JOIN consolidated_layer_bills.committees__members cm ON
    l.bioguide_id = cm.member_bioguide_id
INNER JOIN consolidated_layer_bills.committees c ON
    cm.committee_thomas_id = c.committee_thomas_id

WHERE row_nb::int = ANY(
ARRAY[14842, 14844, 14886, 14887, 501, 502, 4551, 4553, 127, 16521, 3140
	,1539, 1546, 8846, 6346, 4563, 1314, 1383, 4926
	, 4086, 12464, 11982, 16373
	, 318, 455
	, 2936, 2937
	, 17261, 17211])
ORDER BY sw_politician_name, icpsr_politician_id ; 



SELECT *
FROM raw_layer_bills_hc.committee_membership_congress_105_115
WHERE sw_politician_name = 'Wyden, Ron'


SELECT icpsr_politician_id, party_abb, COUNT(*) OVER(PARTITION BY icpsr_politician_id) AS c
FROM raw_layer_bills_hc.committee_membership_congress_105_115
GROUP BY icpsr_politician_id, party_abb 
ORDER BY c DESC, icpsr_politician_id; 

-- export the CSV as text
-- export the elgislator + committee + committee_membership as teext
--
--COPY (SELECt bioguide_id, govtrack_id, first_name, last_name, full_name, gender
--FROM consolidated_layer_bills.legislators as l )
--TO '/tmp/test_storage_size_010_legislators.csv' WITH (FORMAT CSV); 
--
--COPY (SELECT committee_thomas_id, committee_most_recent_name
--FROM consolidated_layer_bills.committees as c )
--TO '/tmp/test_storage_size_020_committees.csv' WITH (FORMAT CSV); 
--
--
--COPY (
--SELECT cm.committee_thomas_id, congress_session, member_bioguide_id, member_start_date, member_rank, member_title
--FROM consolidated_layer_bills.committees__members as cm 
--)TO '/tmp/test_storage_size_030_committee_membership.csv' WITH (FORMAT CSV); 
--
--COPY( 
--SELECT row_nb, newid, std_lname, std_f2initial, chamber, gender, state_abb, district, party_abb, sw_politician_name, gov_politician_name, icpsr_politician_id, govtrack_id, fec_candidate_id, gov_terms, first_yr_in_congress, last_year_in_congress, congress_num, congress_begin_yr, congress_end_yr, icpsr_com_name, icpsr_com_id, lobbyview_com_id, maj_min, rank_within_party, senior_party_member, com_seniority, com_period_of_service, notes 
--FROM raw_layer_bills_hc.committee_membership_congress_105_115
--) TO '/tmp/test_storage_size_999_raw_csv.csv' WITH (FORMAT CSV);
--
-- 

