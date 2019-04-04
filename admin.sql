<<<<<<< HEAD
-- In R, system("psql -f admin.sql") runs this
CREATE SCHEMA se_features;
CREATE ROLE se_features;
CREATE ROLE se_features_access;
ALTER SCHEMA se_features OWNER TO se_features;
GRANT USAGE ON SCHEMA se_features TO se_features_access;
