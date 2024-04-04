
def get_db_creds():
    db_type = input("Enter Your Db_Type")
    database_name = input("Enter Your Database_name")
    server_address   = input("Enter Your Server_address")
    username = input("Enter Your Username")
    password = input("Enter Your Password")
    port = input("Enter Your Port Number If Exists Else Skip ")
    service_name = input("Enter Your Service Name For Oracle/If Other's Skip")
    return db_type, database_name, server_address, username, password, port, service_name


def get_neo_creds():
    uri = input("Enter Your Neo URI")
    neo_username = input("Enter Your Neo Username")
    neo_password = input("Enter Your Neo Password")
    return uri, neo_username, neo_password
