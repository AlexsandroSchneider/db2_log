import psycopg2
import pandas as pd

def read_log(path):
    df = pd.read_csv(path, sep='<', names=['LOG'], engine='python')
    df = df.LOG.str.strip('>')
    dados = [x for x in df]
    check_list = []
    commits_list = []
    index_ckpt = -1
    operations_list = []

    for x in reversed(dados): # Verifica existência do último checkpoint
        if x.startswith('CKPT'):
            index_ckpt = dados.index(x)
            x = x.lstrip('CKPT ')
            check_list = list(map(str, x.replace('(','').replace(')','').split(','))) # Lista as transações à verificar
            break
    #print(check_list) # debug is on the table

    for n, x in enumerate(dados): # Verifica existência de transações commitadas
        if x.startswith('commit'):
            x = x.lstrip('commit ')
            if index_ckpt > -1: # Se há CKPT somente refaz as transações commitadas após o mesmo
                if x in check_list:
                    commits_list.append(x)
                elif n > index_ckpt:
                    commits_list.append(x)
            else: # Se não há CKPT refaz todas as transações commitadas
                commits_list.append(x)
    #print(commits_list) # debug is on the table

    for x in dados:
        if x.startswith('T'): # Se é uma transação e pertence a lista de commits, então deve ser refeita
            operations = list(map(str, x.replace('(','').replace(')','').split(',')))
            if operations[0] in commits_list:
                operations_list.append(operations)
    return check_list, commits_list, operations_list

def initiate_table(conex):
    cursor = conex.cursor()
    query = 'drop table if exists tp_log'
    cursor.execute(query)
    query = 'create table tp_log (id integer, A integer, B integer)'
    cursor.execute(query)
    query = "insert into tp_log values (%s, %s, %s)"
    cursor.execute(query, (1,100,20))
    cursor.execute(query, (2,20,30))
    conex.commit()
    cursor.close()

def check_update(conex, operations):
    meta = []
    cursor = conex.cursor()
    for op in operations: # 4. Verifica se precisa atualizar a tupla
        query = f"select {op[2]} from tp_log where id = {op[1]}"
        cursor.execute(query)
        if cursor.fetchone()[0] != op[3]:
            query = f"update tp_log set {op[2]} = {op[3]} where id = {op[1]}"
            #print(query) # debug is on the table
            cursor.execute(query)
            #print(f"Transação {op[0]} atualizou a coluna {op[2]} da linha {op[1]} para o valor {op[3]}") # debug is on the table
            meta.append({f'"att":"{op[2]}","row":"{op[1]}","value"="{op[3]}"'})
    conex.commit()
    cursor.close()
    return meta

def print_table(conex):
    cursor = conex.cursor()
    cursor.execute('select * from tp_log')
    tuplas = cursor.fetchall()
    for tupla in tuplas:
        print(tupla)

def check_transactions(check, commit):
    for x in check:
        if x in commit:
            print(f"Transação {x} realizou Redo")
        else:
            print(f"Transação {x} não realizou Redo")    

def main():
    ## Definir as informações de conexão do Postgres
    conex = psycopg2.connect(host='localhost', port='7438', database='db2',user='postgres', password='postgres')
    initiate_table(conex) # 1. Carrega o banco de dados com a tabela
    path = './entradalog'
    check, commit, operations = read_log(path) # 2.
    check_transactions(check, commit) # 3.

    metadados = check_update(conex, operations) # 4. 
    print(metadados) # 5.
    print_table(conex)
    conex.close()

if __name__ == "__main__":
    main()