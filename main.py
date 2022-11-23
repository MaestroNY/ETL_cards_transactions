import os
import pandas as pd
import sqlite3
from datetime import datetime
from datetime import timedelta

files_aval = os.listdir('./')

conn = sqlite3.connect('operations.db')
cursor = conn.cursor()

conn2 = sqlite3.connect('BANK.db')
cursor2 = conn2.cursor()


'''ФУНКЦИИ'''
#Функция выцепляет из названия дату
def actual_data(file_name):
	last_pos = file_name.find('.')
	date = file_name[last_pos-8:last_pos]
	date = datetime.strptime(date, '%d%m%Y')
	return date

# Добавление забаненных поспартов
def add_black_passports(stg_passport_blacklist):
	cursor.execute('''
		CREATE TABLE if not exists dwh_fact_passport_blacklist(
			entry_dt date,
			passport_num varchar(30)
		)
	''')

	stg_passport_blacklist.to_sql('stg_passport_blacklist',\
	 conn, if_exists='replace', index=False)

	cursor.execute('''
		INSERT INTO dwh_fact_passport_blacklist
		SELECT
			t1.*
		FROM stg_passport_blacklist t1
		LEFT JOIN dwh_fact_passport_blacklist t2
		on t1.passport = t2.passport_num
		WHERE t2.passport_num is null
	''')

	cursor.execute('''
		DROP TABLE stg_passport_blacklist
	''')

	conn.commit()

# Добавление транзакций
# На всякий случай, как и для паспортов, запишем проверку на наличие в таблице добавляемых значений
def add_transactions(stg_transactions):
	cursor.execute('''
		CREATE TABLE if not exists dwh_fact_transactions(
			trans_id int,
			trans_date date,
			card_num varchar(30),
			oper_type varchar(20),
			amt decimal(20, 2),
			oper_result varchar(20),
			terminal varchar(20)
		)
	''')

	stg_transactions.to_sql('stg_transactions',\
	 conn, if_exists='replace', index=False)

	cursor.execute('''
		INSERT INTO dwh_fact_transactions
		SELECT
			t1.transaction_id,
			t1.transaction_date,
			t1.card_num,
			t1.oper_type,
			t1.amount,
			t1.oper_result,
			t1.terminal
		FROM stg_transactions t1
		LEFT JOIN dwh_fact_transactions t2
		on t1.transaction_id = t2.trans_id
		WHERE t2.trans_id is null
	''')

	cursor.execute('''
		DROP TABLE stg_transactions
	''')

	conn.commit()

# Добавление новых терминалов в формате sqd2
def add_terminals(stg_terminals, date):
	cursor.execute('''
		CREATE TABLE if not exists dwh_dim_terminals_hist(
			terminal_id varchar(20),
			terminal_type varchar(20),
			terminal_city varchar(50),
			terminal_address varchar(120),
			effective_from date,
			effective_to date,
			deleted_flg integer
		)
	''')

	stg_terminals.to_sql('stg_terminals',\
	 conn, if_exists='replace', index=False)

	#Новые строки
	cursor.execute(f'''
		INSERT INTO dwh_dim_terminals_hist
		SELECT
			t1.*,
			datetime('{date}') as effective_from,
			datetime('2999-12-31 23:59:59') as effective_to,
			0 as deleted_flg
		FROM stg_terminals t1
		LEFT JOIN dwh_dim_terminals_hist t2
		ON t1.terminal_id = t2.terminal_id
		WHERE t2.terminal_id is null
	''')

	#Изменение строк
	#здесь заодно учтутся восстановления удаленных терминалов через сравнение флагов
	cursor.execute('''
		CREATE TABLE stg_id_changed AS
		SELECT
			t1.terminal_id
		FROM (
			SELECT
				*,
				0 as deleted_flg
			FROM stg_terminals
			) t1
		INNER JOIN dwh_dim_terminals_hist t2
		ON t1.terminal_id = t2.terminal_id
		AND (
				t1.terminal_type <> t2.terminal_type
			OR t1.terminal_city <> t2.terminal_city
			OR t1.terminal_address <> t2.terminal_address
			OR t1.deleted_flg <> t2.deleted_flg 
		)
		AND t2.effective_to = datetime('2999-12-31 23:59:59')
	''')

	cursor.execute(f'''
		UPDATE dwh_dim_terminals_hist
		SET effective_to = datetime('{date}', '-1 second')
		WHERE terminal_id in stg_id_changed
		AND effective_to = datetime('2999-12-31 23:59:59')
	''')

	cursor.execute(f'''
		INSERT INTO dwh_dim_terminals_hist
		SELECT
			*,
			datetime('{date}') as effective_from,
			datetime('2999-12-31 23:59:59') as effective_to,
			0 as deleted_flg
		FROM stg_terminals
		WHERE terminal_id in stg_id_changed
	''')

	#Удаление строк
	cursor.execute('''
		CREATE TABLE stg_deleted AS
		SELECT
			t1.*
		FROM dwh_dim_terminals_hist t1
		LEFT JOIN stg_terminals t2
		ON t1.terminal_id = t2.terminal_id
		WHERE t2.terminal_id is null
		AND t1.deleted_flg = 0
		AND t1.effective_to = datetime('2999-12-31 23:59:59')
	''')

	cursor.execute(f'''
		UPDATE dwh_dim_terminals_hist
		SET effective_to = datetime('{date}', '-1 second')
		WHERE terminal_id in (
			SELECT
				terminal_id
			FROM stg_deleted
		)
		AND deleted_flg = 0
		AND effective_to = datetime('2999-12-31 23:59:59')
	''')

	cursor.execute(f'''
		UPDATE stg_deleted
		SET deleted_flg = 1,
			effective_from = datetime('{date}')
	''')

	cursor.execute('''
		INSERT INTO dwh_dim_terminals_hist
		SELECT
			*
		FROM stg_deleted
	''')

	cursor.execute('''
		DROP TABLE stg_terminals
	''')

	cursor.execute('''
		DROP TABLE stg_id_changed
	''')

	cursor.execute('''
		DROP TABLE stg_deleted
	''')

	conn.commit()

# Создание базы данных с клиентскими данными
def create_bank_db(acting_cursor, connection):
	with open('ddl_dml.sql', 'r', encoding='utf-8') as new_db:
		new_db_text = new_db.read()
	sql_commands = new_db_text.split(';')
	for command in sql_commands:
		acting_cursor.execute(command)

	connection.commit()

# Чтение таблицы из хранилища BANK
def read_bank_db(table, connection):
	df = pd.read_sql_query(f'''
		SELECT
			*
		FROM {table}
	''', connection)
	return df

def create_table_cards():
	cursor.execute('''
	CREATE TABLE if not exists dwh_dim_cards_hist(
		card_num varchar(30),
		account_num varchar(30),
		effective_from date,
		effective_to date,
		deleted_flg integer
	)
''')

def create_table_accounts():
	cursor.execute('''
	CREATE TABLE if not exists dwh_dim_accounts_hist(
		account_num varchar(30),
		valid_to date,
		client varchar(30),
		effective_from date,		
		effective_to date,
		deleted_flg integer
	)
''')

def create_table_clients():
	cursor.execute('''
	CREATE TABLE if not exists dwh_dim_clients_hist(
		client_id varchar(30),
		last_name varchar(120),
		first_name varchar(120),
		patrinymic varchar(120),
		date_of_birth date,
		passport_num varchar(30),
		passport_valid_to date,
		phone varchar(30),
		effective_from date,		
		effective_to date,
		deleted_flg integer
	)
''')

#Функция на добаление в базу данных таблицы из базы данных BANK в формате sqd2
def add_bank_table(stg_table, table_name, columns_for_code_first, columns_for_code_final, key_first, key_final):
	# Общая логика всех изменений - effective_from у актуальной записи scd2 должна быть как
	# update_dt, если были изменения, и create_dt, если их не было - в scd1
	if table_name == 'cards':
		create_table_cards()
	elif table_name == 'accounts':
		create_table_accounts()
	elif table_name == 'clients':
		create_table_clients()

	stg_table.to_sql('stg_' + table_name,\
		conn, if_exists='replace', index=False)

	#Новые строки
	cursor.execute(f'''
		INSERT INTO dwh_dim_{table_name}_hist
		SELECT
			{columns_for_code_first}
			(CASE 
				WHEN t1.update_dt is null
				THEN t1.create_dt
				ELSE max(t1.create_dt, t1.update_dt)
			END) as effective_from,
			datetime('2999-12-31 23:59:59') as effective_to,
			0 as deleted_flg
		FROM stg_{table_name} t1
		LEFT JOIN dwh_dim_{table_name}_hist t2
		ON t1.{key_first} = t2.{key_final}
		WHERE t2.{key_final} is null
	''')

	#Изменение строк
	#здесь заодно учтутся восстановления удаленных строк через сравнение дат
	cursor.execute(f'''
		CREATE TABLE if not exists stg_updated_rows AS
			SELECT
				{columns_for_code_final}
				t1.effective_from,
				CASE
					WHEN t2.{key_first} is null
					THEN t1.effective_to
					ELSE datetime(CASE
							WHEN t2.update_dt is null
							THEN t2.create_dt
							ELSE max(t2.create_dt, t2.update_dt)
						  END, '-1 second'
					)
				END as effective_to,
				t1.deleted_flg
			FROM dwh_dim_{table_name}_hist t1
			LEFT JOIN stg_{table_name} t2
			ON t1.{key_final} = t2.{key_first}
			AND t1.effective_to = datetime('2999-12-31 23:59:59')
			AND t1.effective_from <> 
				(CASE
					WHEN t2.update_dt is null
					THEN t2.create_dt
					ELSE max(t2.create_dt, t2.update_dt)
				END)
	''')

	cursor.execute(f'''
		DROP TABLE dwh_dim_{table_name}_hist
	''')

	cursor.execute(f'''
		CREATE TABLE if not exists dwh_dim_{table_name}_hist AS
		SELECT
			*
		FROM stg_updated_rows
	''')

	cursor.execute(f'''
		INSERT INTO dwh_dim_{table_name}_hist
		SELECT
			{columns_for_code_first}
			(CASE 
				WHEN t1.update_dt is null
				THEN t1.create_dt
				ELSE max(t1.create_dt, t1.update_dt)
			END) as effective_from,
			datetime('2999-12-31 23:59:59') as effective_to,
			0 as deleted_flg			
		FROM stg_{table_name} t1
		LEFT JOIN dwh_dim_{table_name}_hist t2
		ON t1.{key_first} = t2.{key_final}
		AND t2.effective_to = datetime('2999-12-31 23:59:59')
		AND t2.effective_from = 
			(CASE
				WHEN t1.update_dt is null
				THEN t1.create_dt
				ELSE max(t1.create_dt, t1.update_dt)
			END)
		WHERE t2.{key_final} is null
	''')

	#Удаление строк
	cursor.execute(f'''
		CREATE TABLE stg_deleted_rows AS
		SELECT
			{columns_for_code_final}
			'{date}' as effective_from,
			datetime('2999-12-31 23:59:59') as effective_to,
			1 as deleted_flg
		FROM dwh_dim_{table_name}_hist t1
		LEFT JOIN stg_{table_name} t2
		ON t1.{key_final} = t2.{key_first}
		WHERE t1.effective_to = datetime('2999-12-31 23:59:59')
		AND t2.{key_first} is null
		AND deleted_flg = 0
	''')

	cursor.execute(f'''
		UPDATE dwh_dim_{table_name}_hist
		SET effective_to = datetime('{date}', '-1 second')
		WHERE {key_final} in (
			SELECT
				{key_final}
			FROM stg_deleted_rows
		)
		AND effective_to = datetime('2999-12-31 23:59:59')
	''')

	cursor.execute(f'''
		INSERT INTO dwh_dim_{table_name}_hist
		SELECT 
			*
		FROM stg_deleted_rows
	''')

	cursor.execute(f'''
		DROP TABLE stg_{table_name}
	''')

	cursor.execute('''
		DROP TABLE stg_updated_rows
	''')	

	cursor.execute('''
		DROP TABLE stg_deleted_rows
	''')	

	conn.commit()

def create_rep_fraud():
	cursor.execute('''
		CREATE TABLE if not exists REP_FRAUD(
			event_dt datetime,
			passport varchar(30),
			fio varchar(400),
			phone varchar(30),
			event_type integer,
			report_dt date
		)
	''')

# Поиск фрода с типом 1
def invalid_passport(date):
	cursor.execute(f'''
		INSERT INTO REP_FRAUD
		SELECT
			t1.trans_date AS event_dt,
			t4.passport_num AS passport,
			(t4.last_name || ' ' || t4.first_name || ' ' || t4.patrinymic) AS fio,
			t4.phone,
			1 AS event_type,
			datetime('now') AS report_dt
		FROM dwh_fact_transactions t1
		LEFT JOIN dwh_dim_cards_hist t2
		ON t1.card_num = t2.card_num
		LEFT JOIN dwh_dim_accounts_hist t3
		ON t2.account_num = t3.account_num
		LEFT JOIN dwh_dim_clients_hist t4
		ON t3.client = t4.client_id
		WHERE t1.oper_result = 'SUCCESS'
		AND t1.trans_date < datetime('{date}', '+1 day')
		AND t1.trans_date >= datetime('{date}')
		AND t2.effective_from <= t1.trans_date
		AND t2.effective_to >= t1.trans_date
		AND t3.effective_from <= t1.trans_date
		AND t3.effective_to >= t1.trans_date
		AND t4.effective_from <= t1.trans_date
		AND t4.effective_to >= t1.trans_date
		AND (
			datetime(t4.passport_valid_to, '+1 day') < t1.trans_date
			OR
			t4.passport_num in (
					SELECT
						passport_num
					FROM dwh_fact_passport_blacklist
				)
			)
	''')
	conn.commit()

# Поиск фрода с типом 2
def invalid_agreement(date):
	cursor.execute(f'''
		INSERT INTO REP_FRAUD
		SELECT
			t1.trans_date AS event_dt,
			t4.passport_num AS passport,
			(t4.last_name || ' ' || t4.first_name || ' ' || t4.patrinymic) AS fio,
			t4.phone,
			2 AS event_type,
			datetime('now') AS report_dt
		FROM dwh_fact_transactions t1
		LEFT JOIN dwh_dim_cards_hist t2
		ON t1.card_num = t2.card_num
		LEFT JOIN dwh_dim_accounts_hist t3
		ON t2.account_num = t3.account_num
		LEFT JOIN dwh_dim_clients_hist t4
		ON t3.client = t4.client_id
		WHERE t1.oper_result = 'SUCCESS'
		AND t1.trans_date < datetime('{date}', '+1 day')
		AND t1.trans_date >= datetime('{date}')
		AND t2.effective_from <= t1.trans_date
		AND t2.effective_to >= t1.trans_date
		AND t3.effective_from <= t1.trans_date
		AND t3.effective_to >= t1.trans_date
		AND t4.effective_from <= t1.trans_date
		AND t4.effective_to >= t1.trans_date
		AND datetime(t3.valid_to, '+1 day') < t1.trans_date
	''')
	conn.commit()

# Поиск фрода с типом 3, выгрузка из sql, вычисления - в питоне, затем обратно найденные фродовые
# транзакции в sql
def different_cities(date):
	operations = pd.read_sql_query(f'''
		SELECT
			t1.trans_date AS event_dt,
			t4.passport_num AS passport,
			(t4.last_name || ' ' || t4.first_name || ' ' || t4.patrinymic) AS fio,
			t4.phone,
			3 AS event_type,
			datetime('now') AS report_dt,
			t5.terminal_city
		FROM dwh_fact_transactions t1
		LEFT JOIN dwh_dim_cards_hist t2
		ON t1.card_num = t2.card_num
		LEFT JOIN dwh_dim_accounts_hist t3
		ON t2.account_num = t3.account_num
		LEFT JOIN dwh_dim_clients_hist t4
		ON t3.client = t4.client_id
		LEFT JOIN dwh_dim_terminals_hist t5
		ON t1.terminal = t5.terminal_id
		WHERE t1.oper_result = 'SUCCESS'
		AND t1.trans_date < datetime('{date}', '+1 day')
		AND t1.trans_date >= datetime('{date}', '-1 hour')
		AND t2.effective_from <= t1.trans_date
		AND t2.effective_to >= t1.trans_date
		AND t3.effective_from <= t1.trans_date
		AND t3.effective_to >= t1.trans_date
		AND t4.effective_from <= t1.trans_date
		AND t4.effective_to >= t1.trans_date

	''', conn)

	operations = operations.sort_values(by=['fio', 'event_dt']).reset_index(drop=True)
	operations['event_dt'] = pd.to_datetime(operations['event_dt'])
	operations['fraud'] = 0

	for i in range(len(operations)):
		name = operations.iloc[i, 2]
		time = operations.iloc[i, 0]

		window = operations[(operations['fio'] == name)\
							& (operations['event_dt'] <= time)\
		 					& (operations['event_dt'] >= time - timedelta(hours=1))]

		n_cities = len(window['terminal_city'].unique())

		if n_cities > 1:
			operations.iloc[i, -1] = 1
	
	operations = operations[operations['fraud'] == 1][(operations.columns[:-2].to_list())]
	operations = operations[operations['event_dt'] >= date]
	operations['report_dt'] = datetime.now().strftime('%d-%m-%Y %I:%M:%S')

	operations.to_sql('REP_FRAUD', con=conn, if_exists='append', index=False)

	conn.commit()

# Поиск фрода с типом 4, выгрузка из sql, вычисления - в питоне, затем обратно найденные фродовые
# транзакции в sql
def amount_selection(date):
	operations = pd.read_sql_query(f'''
		SELECT
			t1.trans_date AS event_dt,
			t4.passport_num AS passport,
			(t4.last_name || ' ' || t4.first_name || ' ' || t4.patrinymic) AS fio,
			t4.phone,
			4 AS event_type,
			datetime('now') AS report_dt,
			t1.amt,
			t1.oper_result
		FROM dwh_fact_transactions t1
		LEFT JOIN dwh_dim_cards_hist t2
		ON t1.card_num = t2.card_num
		LEFT JOIN dwh_dim_accounts_hist t3
		ON t2.account_num = t3.account_num
		LEFT JOIN dwh_dim_clients_hist t4
		ON t3.client = t4.client_id
		WHERE t1.trans_date < datetime('{date}', '+1 day')
		AND t1.trans_date >= datetime('{date}', '-20 minutes')
		AND t2.effective_from <= t1.trans_date
		AND t2.effective_to >= t1.trans_date
		AND t3.effective_from <= t1.trans_date
		AND t3.effective_to >= t1.trans_date
		AND t4.effective_from <= t1.trans_date
		AND t4.effective_to >= t1.trans_date

	''', conn)

	operations = operations.sort_values(by=['fio', 'event_dt']).reset_index(drop=True)
	operations['event_dt'] = pd.to_datetime(operations['event_dt'])
	operations['fraud'] = 0

	for i in range(len(operations)):
		name = operations.iloc[i, 2]
		time = operations.iloc[i, 0]

		window = operations[(operations['fio'] == name)\
							& (operations['event_dt'] <= time)\
		 					& (operations['event_dt'] >= time - timedelta(minutes=20))]
		if len(window) > 3:
			window = window.iloc[-4:]
			if (float(window.iloc[0, -3].replace(',', '.')) > float(window.iloc[1, -3].replace(',', '.')))\
			& (float(window.iloc[1, -3].replace(',', '.')) > float(window.iloc[2, -3].replace(',', '.')))\
			& (float(window.iloc[2, -3].replace(',', '.')) > float(window.iloc[3, -3].replace(',', '.')))\
			& (window.iloc[0, -2] == window.iloc[1, -2] == window.iloc[2, -2] == 'REJECT')\
			& (window.iloc[3, -2] == 'SUCCESS'):
				operations.iloc[i, -1] = 1

	operations = operations[operations['fraud'] == 1][(operations.columns[:-3].to_list())]
	operations = operations[operations['event_dt'] >= date]
	operations['report_dt'] = datetime.now().strftime('%d-%m-%Y %I:%M:%S')

	operations.to_sql('REP_FRAUD', con=conn, if_exists='append', index=False)

	conn.commit()

'''ПАРАМЕТРЫ'''
# Сделано для того, что бы не писать функцию по переносу каждой таблицы из BANK с форматом sqd1
# в хранилище operations с форматом sqd2
# Поэтому прописали универсальную функцию, где нужные столбцы подставляются из параметров ниже

# В columns_first - бизнес-поля таблицы в хранилище BANK
# В columns_final - бизнес-поля аналогичной таблицы в хранилище operations, куда переносим данные
cards_columns_first = '''t1.card_num, 
				   		 t1.account,'''
cards_columns_final = '''t1.card_num, 
				   		 t1.account_num,'''	

accounts_columns_first = '''t1.account, 
				   		 t1.valid_to,
				   		 t1.client,'''
accounts_columns_final = '''t1.account_num, 
				   		 t1.valid_to,
				   		 t1.client,'''

clients_columns_first = '''t1.client_id, 
				   		 t1.last_name,
				   		 t1.first_name,
				   		 t1.patronymic,
				   		 t1.date_of_birth,
				   		 t1.passport_num,
				   		 t1.passport_valid_to,
				   		 t1.phone,'''
clients_columns_final = '''t1.client_id, 
				   		 t1.last_name,
				   		 t1.first_name,
				   		 t1.patrinymic,
				   		 t1.date_of_birth,
				   		 t1.passport_num,
				   		 t1.passport_valid_to,
				   		 t1.phone,'''

# В key_first - ключ таблицы в хранилище BANK
# В key_final - ключ аналогичной таблицы в хранилище operations, куда переносим данные
cards_key_first = 'card_num'
cards_key_final = 'card_num'

accounts_key_first = 'account'
accounts_key_final = 'account_num'

clients_key_first = 'client_id'
clients_key_final = 'client_id'

# В name - название таблицы в итоговом хранилище (operations), по итогам работы кода будет название
# dwh_dim_name_hist

cards_name = 'cards'

accounts_name = 'accounts'

clients_name = 'clients'

table_params = {'cards': [cards_name, cards_columns_first, cards_columns_final, cards_key_first,\
						  cards_key_final],
				'accounts': [accounts_name, accounts_columns_first, accounts_columns_final,\
							 accounts_key_first, accounts_key_final],
				'clients': [clients_name, clients_columns_first, clients_columns_final, clients_key_first,\
						  clients_key_final]}

'''КОД ПО ОБНОВЛЕНИЮ ХРАНИЛИЩА'''

# В списках поместим соответствующие файлы из директории
# При певоначальном запуске в списках будет по 3 файла (в папке ранжированы по дате)
# При последующих запусках будет по 1 файлу, т.к. по заданию ежедневно приходит по 1 файлу
passport_blacklists = []
transactions = []
terminals = []
for file in files_aval:
	# Добавляем проверку на отсутствие в названии $, т.к. при выполнении кода может создаваться скрытый
	# временный файл с названием и $, например: ~$terminals_01032021.xlsx
	if file.__contains__('passport_blacklist') and not file.__contains__('$'):
		passport_blacklists.append(file)
	elif file.__contains__('transactions') and not file.__contains__('$'):
		transactions.append(file)
	elif file.__contains__('terminals') and not file.__contains__('$'):
		terminals.append(file)

if 'BANK.db' not in files_aval:
	create_bank_db(cursor2, conn2)

for day in range(len(passport_blacklists)):
	stg_passport_blacklist = pd.read_excel(passport_blacklists[day])
	stg_transactions = pd.read_csv(transactions[day], delimiter=';')
	stg_terminals = pd.read_excel(terminals[day])

	date = actual_data(terminals[day])

	add_black_passports(stg_passport_blacklist)
	add_transactions(stg_transactions)
	add_terminals(stg_terminals, date)

	stg_cards = read_bank_db('cards', conn2)
	stg_accounts = read_bank_db('accounts', conn2)
	stg_clients = read_bank_db('clients', conn2)

	add_bank_table(stg_cards, table_params['cards'][0], table_params['cards'][1],\
					 table_params['cards'][2], table_params['cards'][3], table_params['cards'][4])

	add_bank_table(stg_accounts, table_params['accounts'][0], table_params['accounts'][1],\
					 table_params['accounts'][2], table_params['accounts'][3], table_params['accounts'][4])

	add_bank_table(stg_clients, table_params['clients'][0], table_params['clients'][1],\
					 table_params['clients'][2], table_params['clients'][3], table_params['clients'][4])

	# Перенос отработанных файлов в папку archive
	os.rename(passport_blacklists[day], passport_blacklists[day] + '.backup')
	os.replace(passport_blacklists[day] + '.backup', 'archive/' + passport_blacklists[day] + '.backup')
	os.rename(transactions[day], transactions[day] + '.backup')
	os.replace(transactions[day] + '.backup', 'archive/' + transactions[day] + '.backup')	
	os.rename(terminals[day], terminals[day] + '.backup')
	os.replace(terminals[day] + '.backup', 'archive/' + terminals[day] + '.backup')

	'''КОД ПО СОЗДАНИЮ ОТЧЕТА'''
	create_rep_fraud()
	invalid_passport(date)
	invalid_agreement(date)
	different_cities(date)
	amount_selection(date)

# Примерное время выполнения кода - 5-10 мин
