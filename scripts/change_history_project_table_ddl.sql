EXECUTE IMMEDIATE $$
CREATE OR REPLACE TABLE SCHEMACHANGE.CHANGE_HISTORY_${{ env.SUBDIR }} AS
SELECT *
FROM SCHEMACHANGE.CHANGE_HISTORY
WHERE PROJECT = '${{ env.SUBDIR }}';
$$
