import psycopg2
import pandas as pd

def read_log(log_path):
    df = pd.read_csv(log_path, sep='<', names=['LOG'], engine='python') ## 2° LÊ LOG
    dados = [x for x in df.LOG.str.strip('>')]
    ckpt_list = []
    commits_list = []
    operations_list = []
    starts_after_list = []
    index_ckpt = -1

    for x in reversed(dados): # Verifica existência do último checkpoint (percorre log do fim para o inicio)
        if x.startswith('CKPT'):
            index_ckpt = dados.index(x)
            if len(x.lstrip('CKPT ')) != 0:
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

    transaction_list = check_transactions((ckpt_list + starts_after_list), commits_list) ## 3° VERIFICA REDO
    for x in dados:
        if x.startswith('T'): # Se a operação pertence à uma transação commitada válida, então deve ser verificada
            operation = list(map(str, x.replace('(','').replace(')','').split(',')))
            if operation[0] in transaction_list:
                operations_list.append(operation)
    
    return operations_list

def initiate_table(cursor, metadata_path):
    cursor.execute('drop table if exists tp_log')
    cursor.execute('create table tp_log (id integer, A integer, B integer)')
    df = pd.read_json(metadata_path)['INITIAL']
    for x in range(len(df['A'])):
        cursor.execute('insert into tp_log values (%s, %s, %s)', (x+1 ,df['A'][x], df['B'][x]))

def check_transactions(check, commit):
    transactions = []
    for x in check:
        if x in commit:
            print(f'Transação {x} realizou REDO')
            transactions.append(x)
        else:
            print(f'Transação {x} não realizou REDO.')
    print()
    return transactions

def print_metadata(cursor):
    cursor.execute('select * from tp_log order by id')
    row = cursor.fetchall()
    json = {"INITIAL":{}}
    json["INITIAL"]["A"] = [x[1] for x in row]
    json["INITIAL"]["B"] = [x[2] for x in row]
    print('\nDados após REDO:\n',json)

def check_update(cursor, operations):
    for op in operations:
        cursor.execute(f'select {op[2]} from tp_log where id = {op[1]}')
        if int(cursor.fetchone()[0]) != int(op[4]): ## 4° COMPARA VALORES A ATUALIZAR
            cursor.execute(f'update tp_log set {op[2]} = {op[4]} where id = {op[1]}')
            print(f'Transação {op[0]} atualizou: id = {op[1]}, coluna = {op[2]}, valor = {op[4]}.') ## 5° REPORTA ATUALIZAÇÃO DE DADOS

def main():
    log_path = './entradaLog'
    metadata_path = './metadado.json'
    conn = None
    try:
        conn = psycopg2.connect(host='localhost', port='7438', database='db2',user='postgres', password='postgres')
        cursor = conn.cursor()

        initiate_table(cursor, metadata_path) # 1. Carrega o banco de dados com a tabela
        conn.commit()

        operations = read_log(log_path) # 2 e 3. Lê o log; Verifica e reporta quais transações precisam fazer REDO; RETORNA lista de operações à verificar

        check_update(cursor, operations) # 4 e 5. Checar valores e atualizar se necessário; Reportar atualizações
        conn.commit()

        print_metadata(cursor) # 6. Print metadados após REDO
        cursor.close()

    except psycopg2.DatabaseError as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    main()