import pandas as pd
import json
from neo4j import GraphDatabase
from modules.input import *
from modules.db import *


    

def main():
    db_type, database, server, username, password, port, service_name = get_db_creds()
    if db_type.lower() == "ssms":
        tables, sp, functions, fk = exec_ssms(database, server, username, password)

    elif db_type.lower() == "postgres":
        tables, sp, functions, fk = exec_postgres(database, server, username, password, port)

    elif db_type.lower() == "oracle":
        tables, sp, functions, fk = exec_oracle( database, server, username, password, service_name)


    primary_table = [x[0] for x in fk]
    primary_column = [x[1] for x in fk]
    foreign_table = [x[2] for x in fk]
    foreign_column = [x[3] for x in fk]

    fk = [[(x[0],x[1]),(x[2],x[3])] for x in fk]

    master_dict = {}

    for record in tables:
        table_name = record[1]
        if table_name in master_dict.keys():
            fkv=[]
            for k,v in fk:
                # print(k,table_name,record[2])
                if (table_name,record[2])==k:
                    fkv.append(v[0])
            master_dict[table_name]['col'].append({record[2]:{"dtype":record[3],"length":record[4],"is_null":record[5],"fk":fkv}})
        else:
            fkv=[]
            for k,v in fk:
                if (table_name,record[2])==k:
                    fkv.append(v[0])
            master_dict[table_name]={}
            master_dict[table_name]['col'] = [{record[2]:{"dtype":record[3],"length":record[4],"is_null":record[5],"fk":fkv}}]

    for table in master_dict.keys():
        sps = []
        for proc in sp:
            if table in proc[1]:
                sps.append(proc[0])
        # print(table,sps)
        master_dict[table]['stored_procedures']=sps
        
    for table in master_dict.keys():
        funcs = []
        for func in functions:
            if table in func[1]:
                funcs.append(func[0])
        master_dict[table]['functions']=funcs
    
    dtype_ = []
    for table_name, table_data in master_dict.items():
        for column_info in table_data['col']:
            for column_name in column_info.keys():
                    dtype_.append(column_info[column_name]['dtype'])
    dtype_ = set(dtype_)

    length_ = []
    for table_name, table_data in master_dict.items():
        for column_info in table_data['col']:
            for column_name in column_info.keys():
                    if column_info[column_name]['length'] is None:
                        length_.append('NULL')
                    else:
                        length_.append(column_info[column_name]['length'])
    length_ = set(length_)

    is_null_ = []
    for table_name, table_data in master_dict.items():
        for column_info in table_data['col']:
            for column_name in column_info.keys():
                    is_null_.append(column_info[column_name]['is_null'])
    is_null_ = set(is_null_)

    with open('op.json','w') as file:
        file.write(json.dumps(master_dict))

    uri, neo_username, neo_password = get_neo_creds()

    driver = GraphDatabase.driver(uri, auth=(neo_username, neo_password))
    with open('op.json', "r") as file:
        json_data = file.read()

    for data in dtype_:
        with driver.session() as session:
            session.run("CREATE (d:DType {type: $data})", data=data)
    
    for len_ in length_:
        with driver.session() as session:
            session.run("CREATE (l:Length {length: $len_})", len_=len_)

    for isnul in is_null_:
        with driver.session() as session:
            session.run("CREATE (i:Isnull {value: $isnul})", isnul=isnul)

    sp_=[]
    for s in sp:
        sp_.append(s[0])
    sp_ = set(sp_)

    for sp_name in sp_:
        with driver.session() as session:
            session.run("CREATE (s:SP {name: $sp_name})", sp_name=sp_name)

    fn_=[]
    for f in functions:
        fn_.append(f[0])
        
    fn_ = set(fn_)

    for fn_name in fn_:
        with driver.session() as session:
            session.run("CREATE (f:Function {name: $fn_name})", fn_name=fn_name)
            

    schema = json.loads(json_data)

    for table_name, table_data in schema.items():
        with driver.session() as session:
            session.run("CREATE (n:Table {name: $table_name})", table_name=table_name)  
        for column_info in table_data['col']:
            for column_name in column_info.keys():
                if column_name not in primary_column and foreign_column:
                    with driver.session() as session:
                        session.run("""
                            MATCH (n:Table {name: $table_name})
                            CREATE (n)-[:HAS_COLUMN]->(o:Column {name: $column_name})
                        """, table_name=table_name, column_name=column_name)
                else:
                    continue
                    
    pk=[]
    for i, j, k in zip(primary_table, primary_column, foreign_table):
        if j not in pk:
            with driver.session() as session:
                session.run("""
                    MATCH (n:Table {name: $i})
                    CREATE (n)-[:Primary_Key]->(o:Column {name: $j})
                    """, i=i, j=j)
                session.run("""
                    MATCH (p:Table {name: $k}), (o:Column {name: $j})
                    CREATE (o)-[:HAS_FOREIGN_KEY]->(p)
                    """, k=k, j=j)
                pk.append(j)
        else:
            with driver.session() as session:
                session.run("""
                        MATCH (p:Table {name: $k}), (o:Column {name: $j})
                        CREATE (o)-[:HAS_FOREIGN_KEY]->(p)
                        """, k=k, j=j)

    for table_name, table_data in schema.items():
        for column_info in table_data['col']:
            for column_name in column_info.keys():

                with driver.session() as session:
                    session.run("""
                        MATCH (o:Column {name: $column_name}), (d : DType {type: $dtype})
                        MERGE (o)-[:HAS_DTYPE]->(d)
                    """, column_name=column_name, dtype=column_info[column_name]['dtype'])


            
 
                with driver.session() as session:
                    session.run("""
                        MATCH (o:Column {name: $column_name}), (i:Isnull {value: $isnul})
                        MERGE (o)-[:HAS_DTYPE]->(i)
                    """, column_name=column_name, isnul=column_info[column_name]['is_null'])



                length_value = column_info[column_name]['length']
                if length_value is not None:
                        with driver.session() as session:
                            session.run("""
                                MATCH (o:Column {name: $column_name}), (l:Length {length: $len_})
                                MERGE (o)-[:HAS_LENGTH]->(l)
                            """, column_name=column_name, len_=length_value)
                else:
                        length_value = 'NULL'

                        with driver.session() as session:
                            session.run("""
                                    MATCH (o:Column {name: $column_name}), (l:Length {length: $len_})
                                    MERGE (o)-[:HAS_LENGTH]->(l)
                                """, column_name=column_name, len_=length_value)



    with driver.session() as session:
        for sp in table_data['stored_procedures']:
            session.run("""
                    MATCH (n:Table {name: $table_name}), (s : SP {name: $sp})
                    MERGE (n)-[:HAS_sp]->(s)
                """, sp=sp, table_name=table_name)


    with driver.session() as session:
        for func in table_data['functions']:
            session.run("""
                    MATCH (n:Table {name: $table_name}), (f :Function {name: $func})
                    MERGE (n)-[:HAS_fn]->(f)
                """,func = func, table_name=table_name)

    driver.close()              

main()