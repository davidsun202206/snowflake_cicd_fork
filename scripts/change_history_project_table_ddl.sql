CREATE OR REPLACE TABLE SCHEMACHANGE.CHANGE_HISTORY_$SUBDIR AS
SELECT *
FROM SCHEMACHANGE.CHANGE_HISTORY
WHERE PROJECT = '$SUBDIR';
