CREATE OR REPLACE TABLE SCHEMACHANGE.CHANGE_HISTORY_{PROJECT} AS SELECT * FROM SCHEMACHANGE.CHANGE_HISTORY WHERE PROJECT = {PROJECT}
