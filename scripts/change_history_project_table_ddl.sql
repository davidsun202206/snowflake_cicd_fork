CREATE OR REPLACE TABLE SCHEMACHANGE.CHANGE_HISTORY_${subdir} AS SELECT * FROM SCHEMACHANGE.CHANGE_HISTORY WHERE PROJECT = :subdir
