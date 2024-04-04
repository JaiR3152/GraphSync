import pyodbc
import psycopg2
import cx_Oracle


def exec_ssms(database, server, username, password):
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(connection_string)
    
    cursor = conn.cursor()

    cursor.execute('''SELECT 
        TABLE_SCHEMA,
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH,
        IS_NULLABLE
    FROM 
        INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_SCHEMA NOT IN ('sys', 'information_schema') AND
        TABLE_NAME NOT LIKE 'sysdiagrams' ''')

    tables = cursor.fetchall()

    cursor.execute("""
        SELECT o.name, m.definition  
        FROM sys.objects o
        INNER JOIN sys.sql_modules m ON o.object_id = m.object_id
        WHERE o.type = 'P' -- P stands for Stored Procedure
        AND o.is_ms_shipped = 0 -- Exclude system procedures
        AND o.name NOT IN ('sp_helpdiagrams', 'sp_helpdiagramdefinition', 'sp_creatediagram', 'sp_renamediagram', 'sp_alterdiagram', 'sp_dropdiagram', 'sp_upgraddiagrams') -- Exclude specific procedures
    """)

    sp = cursor.fetchall()


    cursor.execute("""
        SELECT o.name, m.definition
        FROM sys.objects o
        INNER JOIN sys.sql_modules m ON o.object_id = m.object_id
        WHERE o.type IN ('FN', 'IF', 'TF') 
        AND o.is_ms_shipped = 0
        AND o.type_desc LIKE 'SQL_%FUNCTION%' 
        AND o.name NOT LIKE 'fn[_]%'
    """)

    functions = cursor.fetchall()

    
    cursor.execute("""
        SELECT 
        PK.TABLE_NAME AS 'Primary Table',
        PK.COLUMN_NAME AS 'Primary Column',
        FK.TABLE_NAME AS 'Foreign Table',
        FK.COLUMN_NAME AS 'Foreign Column'
    FROM 
        INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC
    INNER JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS FK 
        ON FK.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
    INNER JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS PK 
        ON PK.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
    WHERE 
        PK.TABLE_SCHEMA = 'dbo' AND
        FK.TABLE_SCHEMA = 'dbo'
    """)

    fk = cursor.fetchall()

    cursor.close()

    print(tables, sp, functions, fk)
    return tables, sp, functions, fk

def exec_postgres(database, server, username, password, port):
    connection_string = f"dbname={database} user={username} password={password} host={server} port={port}"
    conn = psycopg2.connect(connection_string)
    
    cursor = conn.cursor()

    cursor.execute("""SELECT 
        c.TABLE_SCHEMA,
        c.TABLE_NAME,
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.CHARACTER_MAXIMUM_LENGTH,
        c.IS_NULLABLE
    FROM 
        INFORMATION_SCHEMA.COLUMNS c
    INNER JOIN
        INFORMATION_SCHEMA.TABLES t ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
    WHERE 
        t.TABLE_TYPE = 'BASE TABLE' 
        AND t.TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog') -- Exclude system schemas
    """)

    tables = cursor.fetchall()

    cursor.execute("""SELECT proname AS "Procedure Name",
        prosrc AS "Procedure Code"
    FROM pg_proc p
        LEFT JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public' -- Schema filter, adjust as needed
        AND pg_function_is_visible(p.oid)
        AND prorettype = 'pg_catalog.void'::regtype;
    """)

    sp = cursor.fetchall()

    cursor.execute("""
        SELECT proname AS "Function Name",
        prosrc AS "Function Code"
    FROM pg_proc p
        LEFT JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public' -- Schema filter, adjust as needed
        AND pg_function_is_visible(p.oid);

    """)

    functions = cursor.fetchall()


    cursor.execute("""
        SELECT
        c.conrelid::regclass::text AS "Primary Table",
        array_agg(a.attname) AS "Primary Column",
        c.confrelid::regclass::text AS "Foreign Table",
        array_agg(a2.attname) AS "Foreign Column"
    FROM
        pg_constraint c
    JOIN
        pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    JOIN
        pg_attribute a2 ON a2.attrelid = c.confrelid AND a2.attnum = ANY(c.confkey)
    WHERE
        c.contype = 'f' -- Foreign key constraint
        AND c.connamespace = 'public'::regnamespace -- Schema filter, adjust as needed
    GROUP BY
        "Primary Table", "Foreign Table";
    """)

    fk = cursor.fetchall()

    cursor.close()

    return tables, sp, functions, fk

    

def exec_oracle(server, username, password, port, service_name):
    dsn = cx_Oracle.makedsn(host = server, port = port , service_name = service_name)
    conn = cx_Oracle.connect(username, password, dsn)

    cursor = conn.cursor()

    cursor.execute('''SELECT *
        FROM ALL_TAB_COLUMNS
        WHERE OWNER NOT IN ('MDSYS', 'CTXSYS', 'SYSTEM', 'APEX_040000', 'XDB', 'SYS', 'SYSBACKUP', 'SYSDBA', 
        'SYSOPER', 'OUTLN', 'DBSNMP', 'APPS', 'HR', 'SCOTT', 'APPQOSSYS', 'FLOWS_FILES');
''')

    tables = cursor.fetchall()

    cursor.execute("""
        SELECT
    OWNER,
    OBJECT_NAME
    FROM
        ALL_PROCEDURES
    WHERE
        OWNER NOT IN (
            'MDSYS',
            'CTXSYS',
            'SYSTEM',
            'APEX_040000',
            'XDB',
            'SYS',
            'APPQOSSYS',
            'DBSNMP',
            'OUTLN', 
            'FLOWS_FILES'
        )
        AND OBJECT_TYPE = 'PROCEDURE';""")

    sp = cursor.fetchall()


    cursor.execute("""
        SELECT DISTINCT p.OWNER, p.OBJECT_NAME
    FROM ALL_PROCEDURES p
    LEFT JOIN ALL_ARGUMENTS a
    ON p.OWNER = a.OWNER
    AND p.OBJECT_NAME = a.OBJECT_NAME
    AND a.ARGUMENT_NAME IS NULL
    WHERE p.OWNER NOT IN ('SYS','SYSTEM','MDSYS','CTXSYS','APEX_040000','XDB','APPQOSSYS','DBSNMP','OUTLN','FLOWS_FILES')
    AND p.OBJECT_TYPE = 'FUNCTION';
    """)

    functions = cursor.fetchall()

    
    cursor.execute("""
        SELECT
       pk.table_name AS primary_table,
       pcc.column_name AS primary_column, 
       fk.table_name AS foreign_table,
       fcc.column_name AS foreign_column
    FROM all_constraints fk
    JOIN all_cons_columns fcc 
        ON fk.owner = fcc.owner 
        AND fk.constraint_name = fcc.constraint_name
    JOIN all_constraints pk 
        ON fk.r_owner = pk.owner 
        AND fk.r_constraint_name = pk.constraint_name
    JOIN all_cons_columns pcc 
        ON pk.owner = pcc.owner 
        AND pk.constraint_name = pcc.constraint_name
    WHERE fk.constraint_type = 'R' -- R denotes foreign key constraint
    AND fk.owner NOT IN ('SYS','SYSTEM','MDSYS','CTXSYS','APEX_040000','XDB','APPQOSSYS','DBSNMP','OUTLN','FLOWS_FILES');
    """)

    fk = cursor.fetchall()

    cursor.close()

    return tables, sp, functions, fk
