CREATE OR REPLACE PROCEDURE public.insert_into_final_table()
AS $$
DECLARE 

final_tab_col varchar(500);
stg_tab_col varchar(500);
query_statement varchar(1000);

BEGIN 
	
	final_tab_col := 'Incident_Num,
					 Inident_Description,
					 Day_Of_Week,
					 Date_of_Incident,
					 Time_of_Incident';
					
	stg_tab_col := 'cast(Incident_Num as bigint) as Incident_Num,
					Inident_Description,
					Day_Of_Week,
					cast(Date_of_Incident as timestamp) as Date_of_Incident,
					Time_of_Incident';
				
	query_statement := 'INSERT INTO public.property_incident_details_final ('||final_tab_col||')
						SELECT '||stg_tab_col||' FROM public.property_incident_details_stg;';
					
	RAISE INFO 'Loading data from Stage to Final table...';
	EXECUTE query_statement;

	EXCEPTION 
	  WHEN OTHERS THEN
	     RAISE INFO '%', SQLERRM;
	     RAISE INFO '%', SQLSTATE;
	
	
END;
$$ LANGUAGE plpgsql;