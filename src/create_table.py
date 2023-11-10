from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])

session = cluster.connect()

create_keyspace = """
    CREATE KEYSPACE IF NOT EXISTS itau_shop
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
"""

create_table_query = """
    CREATE TABLE IF NOT EXISTS itau_shop.pedidos (
        txt_detl_idt_pedi_pgto TEXT PRIMARY KEY,
        cod_idef_clie TEXT,
        dat_hor_tran TEXT,
        list_item_pedi list<TEXT>,
        stat_pedi TEXT
    );
"""

session.execute(create_keyspace)
session.execute(create_table_query)
