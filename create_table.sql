CREATE TABLE exoplanets (
     id 		INT PRIMARY KEY NOT NULL,
     kepid		INTEGER		NOT NULL,
     kepoi_name		VARCHAR(10)	NOT NULL,
     kepler_name	VARCHAR(20),
     koi_disposition	VARCHAR(20),
     koi_period		NUMERIC,
     koi_time		NUMERIC,
     koi_duration	NUMERIC,
     koi_ror		NUMERIC,
     koi_prad		NUMERIC,
     koi_teq		NUMERIC,
     koi_dor		NUMERIC,
     koi_count		INTEGER,
     koi_steff		NUMERIC,
     koi_srad		NUMERIC,
     koi_smass		NUMERIC
);
