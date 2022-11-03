import psycopg2
import pandas as pd

def read_log(path):

    df = pd.read_csv(path, sep='<', names=['LOG'], engine='python')
    df = df.LOG.str.strip('>')

    dados = [x for x in df]
    ckpt_list = []
    commits_list = []
    index_ckpt = -1
    operations_list = []

    for x in reversed(dados): # Verifica existência do último checkpoint
        if x.startswith('CKPT'):
            index_ckpt = dados.index(x)
            x = x.lstrip('CKPT ')
            ckpt_list = list(map(str, x.replace('(','').replace(')','').split(','))) # Lista as transações à verificar
            break
    
    print(ckpt_list) # debug is on the table

    for n, x in enumerate(dados): # Verifica existência de transações commitadas
        if x.startswith('commit'):
            x = x.lstrip('commit ')
            if index_ckpt > -1: # Se há CKPT somente refaz as transações commitadas após o mesmo
                if x in ckpt_list:
                    commits_list.append(x)
                elif n > index_ckpt:
                    commits_list.append(x)
            else: # Se não há CKPT refaz todas as transações commitadas
                commits_list.append(x)
    
    print(commits_list) # debug is on the table

    for x in dados:
        if x.startswith('T'): # Se é uma transação e pertence a lista de commits, então deve ser refeita
            operations = list(map(str, x.replace('(','').replace(')','').split(',')))
            if operations[0] in commits_list:
                operations_list.append(operations)

    return operations_list


path = './entradalog'
operations = read_log(path)

## Definir as informações de conexão do Postgres
conex = psycopg2.connect(host='localhost', port='7438', database='db2',user='postgres', password='postgres')

cursor = conex.cursor()
sql = 'drop table if exists tp_log'
cursor.execute(sql)
sql = 'create table tp_log (id integer, A integer, B integer)'
cursor.execute(sql)
sql = "insert into tp_log values (1, 100, 20)"
cursor.execute(sql)
sql = "insert into tp_log values (2, 20, 30)"
cursor.execute(sql)

for x in operations:
    sql = f"update tp_log set {x[2]} = {x[3]} where id = {x[1]}"
    print(sql) # debug is on the table
    cursor.execute(sql)

conex.commit()
cursor.execute('select * from tp_log')

recset = cursor.fetchall()
for rec in recset:
    print(rec)
conex.close()