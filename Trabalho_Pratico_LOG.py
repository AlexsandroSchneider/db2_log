import psycopg2
import pandas as pd

def read_log(path):
    df = pd.read_csv(path, sep='<', names=['LOG'], engine='python') ## 2°
    dados = [x for x in df.LOG.str.strip('>')]
    ckpt_list = []
    commits_list = []
    operations_list = []
    starts_after_list = []
    index_ckpt = -1

    for x in reversed(dados): # Verifica existência do último checkpoint (percorre log do fim para o inicio)
        if x.startswith('CKPT'):
            index_ckpt = dados.index(x)
            ckpt_list = list(map(str, x.lstrip('CKPT ').replace('(','').replace(')','').split(','))) # Lista as transações ativas no CKPT
            break
    for n, x in enumerate(dados):
        if x.startswith('start'): # Verifica transações startadas após CKPT (todas, se não há CKPT)
            if n > index_ckpt:
                starts_after_list.append(x.lstrip('start '))
        elif x.startswith('commit'): # Verifica existência de transações commitadas
            if index_ckpt != -1: # Se há CKPT somente refaz as transações commitadas após o mesmo
                if n > index_ckpt:
                    commits_list.append(x.lstrip('commit '))
            else: # Se não há CKPT refaz todas as transações commitadas
                commits_list.append(x.lstrip('commit '))
    transaction_list = check_transactions((ckpt_list + starts_after_list), commits_list) ## 3°
    for x in dados:
        if x.startswith('T'): # Se a operação pertence à uma transação commitada válida, então deve ser verificada
            operation = list(map(str, x.replace('(','').replace(')','').split(',')))
            if operation[0] in transaction_list:
                operations_list.append(operation)
    
    #print("CheckPt: ", ckpt_list)
    #print("Start After: ", starts_after_list)
    #print("Commit After: ", commits_list)
    #print("Transactions: ", transaction_list)
    #print("Op: ", operations_list)
    return operations_list

def initiate_table(conex):
    cursor = conex.cursor()
    cursor.execute('drop table if exists tp_log')
    cursor.execute('create table tp_log (id integer, A integer, B integer)')
    query = "insert into tp_log values (%s, %s, %s)"
    cursor.execute(query, (1,100,20))
    cursor.execute(query, (2,20,30))
    conex.commit()
    cursor.close()

def check_transactions(check, commit):
    transactions = []
    for x in check:
        if x in commit:
            print(f"Transação {x} realizou REDO")
            transactions.append(x)
        else:
            print(f"Transação {x} não realizou REDO.")
    print()
    return transactions

def print_table(conex):
    print("\nSELECT * FROM TP_LOG:")
    cursor = conex.cursor()
    cursor.execute('select * from tp_log order by id')
    for x in cursor.fetchall():
        print(x)
    cursor.close()

def check_update(conex, operations):
    cursor = conex.cursor()
    for op in operations:
        cursor.execute(f"select {op[2]} from tp_log where id = {op[1]}")
        if int(cursor.fetchone()[0]) != int(op[3]): ## 4°
            cursor.execute(f"update tp_log set {op[2]} = {op[3]} where id = {op[1]}")
            print(f"Transação {op[0]} atualizou: id = {op[1]}, coluna = {op[2]}, valor = {op[3]}.") ## 5°
    conex.commit()
    cursor.close()

def main():
    log_path = './entradaLog2 copy'
    meta_path = './metadado.json' # TODO
    conn = None

    try:
        conex = psycopg2.connect(host='localhost', port='7438', database='db2',user='postgres', password='postgres')
        initiate_table(conex) # 1. Carrega o banco de dados com a tabela
        operations = read_log(log_path) # 2 e 3. Lê o log; Verifica quais transações precisam fazer REDO; RETORNA lista de operações à verificar
        check_update(conex, operations) # 4 e 5. Checar valores e atualizar se necessário; Reportar atualizações
        print_table(conex) # 6. Print tabela após REDO

    except psycopg2.DatabaseError as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()

## TODO inserir dados na tabela a partir do metadados