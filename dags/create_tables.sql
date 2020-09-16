CREATE TABLE IF NOT EXISTS public.DIM_I94_PORT (
	poe_code varchar(20),
    city varchar(256),
    state_or_country varchar(256),
    CONSTRAINT poe_pk PRIMARY KEY (poe_code) 
)
diststyle all;

CREATE TABLE IF NOT EXISTS public.DIM_I94_VISA  (
	visa_code int4,
    visa_category varchar(100),
    CONSTRAINT visa_pk PRIMARY KEY (visa_code)
)
diststyle all;

CREATE TABLE IF NOT EXISTS public.DIM_I94_MODE (
	travel_mode int4,
    mode_name varchar(100),
    CONSTRAINT travel_pk PRIMARY KEY (travel_mode)
)
diststyle all;

CREATE TABLE IF NOT EXISTS public.DIM_I94_ADDR (
	state_code varchar(20) distkey,
    state_name varchar(100),
    CONSTRAINT state_pk PRIMARY KEY (state_code)
);

CREATE TABLE IF NOT EXISTS public.DIM_I94_CIT_RES (
	cit_res_id int4,
    country_name varchar(256),
    CONSTRAINT cit_res_pk PRIMARY KEY (cit_res_id)
)
diststyle all;

CREATE TABLE IF NOT EXISTS public.DIM_TEMPERATURE (
	"month" int4,
    avg_temp float8,
	state_code varchar(10)  distkey,
	state_name varchar(100),
    CONSTRAINT demo_pk PRIMARY KEY (state_code, "month")
);


CREATE TABLE IF NOT EXISTS public.DIM_DEMOGRAPHICS (
	state_name varchar(100)  distkey,
    state_code varchar(10),
	median_age float8,
    male_pop int8,
    female_pop int8,
    total_pop int8,
    no_of_vets int8,
    foreign_born int8,
    avg_household_size float8,
    amer_ind_ak_native int8,
    asian int8,
    black int8,
    hisp_latino int8,
    white int8,
    CONSTRAINT demo_pk PRIMARY KEY (state_code)
);

CREATE TABLE IF NOT EXISTS public.FACT_IMMIGRATION (
  cicid int4,         
  i94yr int4,        
  i94mon int4,  
  i94cit int4,    
  i94res int4,      
  i94port varchar(20),  
  arrdate varchar(20),      
  i94mode  int4,  
  i94addr varchar(20)  distkey,  
  depdate varchar(20),        
  age int4,        
  i94visa int4,     
  dtadfile varchar(256),   
  visapost  varchar(20),     
  biryear int4,         
  dtaddto varchar(256),    
  gender  varchar(20),     
  airline  varchar(20),     
  admnum int4,          
  fltno  varchar(20),     
  visatype  varchar(20) 
);









