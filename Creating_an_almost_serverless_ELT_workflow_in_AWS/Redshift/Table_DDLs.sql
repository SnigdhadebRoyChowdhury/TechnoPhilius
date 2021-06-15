/*DDL for Stage table*/
CREATE TABLE public.property_incident_details_stg
(
	Incident_Num varchar(50),
	Inident_Description varchar(50),
	Day_Of_Week varchar(50),
	Date_of_Incident varchar(50),
	Time_of_Incident varchar(50)
)
DISTKEY(Inident_Description);




/*DDL for Final table*/
CREATE TABLE public.property_incident_details_final
(
	Incident_Num bigint,
	Inident_Description varchar(50),
	Day_Of_Week varchar(50),
	Date_of_Incident timestamp without time zone,
	Time_of_Incident varchar(50)
)
DISTKEY(Inident_Description);