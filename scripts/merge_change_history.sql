MERGE INTO SCHEMACHANGE.CHANGE_HISTORY target
USING SCHEMACHANGE.CHANGE_HISTORY_$subdir source 
ON target.SCRIPT = source.SCRIPT AND target.PROJECT = $env.SUBDIR 
WHEN MATCHED AND (target.SCRIPT_TYPE = 'A' OR (target.SCRIPT_TYPE = 'R' AND target.CHECKSUM != source.CHECKSUM))
    THEN UPDATE SET 
    target.CHECKSUM = source.CHECKSUM
    ,target.EXECUTION_TIME = source.EXECUTION_TIME
    ,target.STATUS = source.STATUS
    ,target.INSTALLED_BY = source.INSTALLED_BY
    ,target.INSTALLED_ON = source.INSTALLED_ON
    ,target.PROJECT = $env.SUBDIR          
WHEN NOT MATCHED 
    THEN INSERT (
    VERSION
    ,DESCRIPTION
    ,SCRIPT
    ,SCRIPT_TYPE
    ,CHECKSUM
    ,EXECUTION_TIME
    ,STATUS
    ,INSTALLED_BY
    ,INSTALLED_ON
    ,PROJECT
    ) 
    VALUES (
    source.VERSION
    ,source.DESCRIPTION
    ,source.SCRIPT
    ,source.SCRIPT_TYPE
    ,source.CHECKSUM
    ,source.EXECUTION_TIME
    ,source.STATUS
    ,source.INSTALLED_BY
    ,source.INSTALLED_ON
    ,$env.SUBDIR
    );
    DROP TABLE SCHEMACHANGE.CHANGE_HISTORY_$subdir;