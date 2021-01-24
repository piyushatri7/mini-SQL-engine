import sys
import csv
import re		#rergexp
import os
import sqlparse

#####################################################################################
def parseMetaData():
	fin = open('metadata.txt','r')
	tableMeta = {}
	cur_table_cols = []
	newtable = False
	cur_table_name = ""

	for line in fin:
		if line.strip() == "<begin_table>":
			cur_table_cols = []
			newtable = True
		elif line.strip() == "<end_table>":
			tableMeta[cur_table_name] = cur_table_cols
		elif newtable == True:
			cur_table_name = line.strip()
			newtable = False
		else:
			cur_table_cols.append(line.strip())

	return tableMeta


######################################################################################
#                 res,temp
def doCrossProduct(X , Y):
	product = []
	#print(X)
	#print()
	#print(Y)
	for i in range(len(X)):
		for j in range(len(Y)):
			row = []
			row.extend(X[i])
			row.extend(Y[j])
			product.append(row)
	return product
#####################################################################################
def getCrossProduct(query_tables , metadata):
	res_cols = []		#store list of columns for cross product
	res_table = []
	for table in query_tables:					
		dbfile =  table + '.csv'

		if os.path.exists(dbfile):
			temp = []
			with open(dbfile) as csv_file:
				csv_reader = csv.reader(csv_file)
				for row in csv_reader:				#store table in temp
					row = [int(i) for i in row] 	#convert all to int
					temp.append(row)
					#print(row)
			#print(temp)

			res_cols.extend(metadata[table])	#column heading for tables

			if len(query_tables) == 1:		#only one output table exists
				return temp , res_cols
			elif len(res_table) == 0:				#multiple table exist but this is the first one to execute
				res_table = temp
			else:							#multiple tables therefore cross product required
				res_table = doCrossProduct(res_table,temp)

		else:
			print("table not found!!")

	return res_table , res_cols


#####################################################################################
def print_op_cols(product_table , product_cols , output_columns , metadata):
	op_idx = []

	try:
		if len(product_table)==0:		#no rows selected
			output_columns = product_cols
		if output_columns[0]=="*":
			output_columns = product_cols		# for * output will be all cols
			op_idx = list(range(len(product_table[0])))
		else:
			for cols in output_columns:
				op_idx.append(product_cols.index(cols))

		#print(output_columns)
		tab_col = []
		for col in output_columns:
				
			if col.find('(') != -1 and col.find(')') != -1:	 	#agg func
				col = col.replace('(',' ')	#max(A)-->max A)
				col = col.replace(')','')		#max(A)-->max A
				col = col.split()[1]			#max A--> A
				col = col.strip()	#remove spaces if any
			#print(col)

			if col=="*":			#case for count(*)
				tab_col.append(col)
			else:
				for x in metadata:		#find which table the column belongs to
					if col in metadata[x]:
						tab_col.append(str(x) + "." + str(col))
						break

		s=""
		for x in tab_col:
			s+=(str(x)+',')
		s = s[:-1]				#remove last comma

		print("<" + s + ">")		#comma separated table.colname
		for row in product_table:
			temp = []
			for i in op_idx:
				temp.append(row[i])
			s = ','.join([str(i) for i in temp])	#convert int to comma separated str
			print(s)

		#print("\n" + str(len(product_table)) + " rows affected")	
	except:
		print("invalid output column name!!!")
		exit()


#####################################################################################
def orderBy(product_table , product_cols , order_params):
	order_params = order_params.split()
	#print(order_params)

	try:
		order_by_col = order_params[0];
		order_col_idx = product_cols.index(order_by_col)
		
		if len(order_params)>1 and order_params[1].lower() == "desc":	#only 1 order_by parameter means by default sort in ascending
			product_table.sort(key = lambda x: int(x[order_col_idx]) , reverse = True)
		else:
			product_table.sort(key = lambda x: int(x[order_col_idx]))

		return product_table

	except:
		print("invalid order by column!!!")
		exit()


#####################################################################################
def distinctCols(product_table , product_cols ,  output_columns):
	op_idx = []
	distinct_table = []
	try:
		if output_columns[0]=="*":
			op_idx = list(range(len(product_table[0])))	
		else:
			for cols in output_columns:
				'''
				if cols.find('(') != -1 and cols.find(')') != -1:	 	#agg func
					cols = cols.replace('(',' ')	#max(A)-->max A)
					cols = cols.replace(')','')		#max(A)-->max A
					cols = cols.split()[1]			#max A--> A
				print(cols)
				'''
				op_idx.append(product_cols.index(cols))		#store indexes of req cols


		for row in product_table:
			temp = []
			for i in op_idx:
				temp.append(row[i])
			distinct_table.append(temp)

		#find distinct rows
		distinct_table = [list(x) for x in set(tuple(x) for x in distinct_table)]
		return distinct_table

	except:
		print("invalid distinct output column name!!!")
		exit()


#####################################################################################
def findOperator(condition):
	ops = ['<=','>=','=','<','>']
	for op in ops:
		if op in condition:
			return op
#####################################################################################
def isInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
#####################################################################################
def whereConditon(conditions , product_table , product_cols):
	#print(conditions)
	andor = ""		#checks if 'and' or 'or' operator is there in where clause

	if re.search("and", conditions, re.IGNORECASE):
		conditions = re.split('and', conditions,flags=re.IGNORECASE)			#make list of conditions
		conditions = [x.strip() for x in conditions]	#remove whitespaces
		andor = "and"

	elif re.search("or", conditions, re.IGNORECASE):
		conditions = re.split('or', conditions,flags=re.IGNORECASE)			#make list of conditions
		conditions = [x.strip() for x in conditions]	#remove whitespaces
		andor = "or"

	else:					#only 1 condition is there
		conditions = [conditions.strip()]

	op1 = findOperator(conditions[0])		#find operator present in condition1
	con1 = conditions[0].split(op1)			#seaprate operands
	con1 = [x.strip() for x in con1]		#remove whitespaces
	if op1 == '=':		#convert SQL '=' to '=='			
		op1 = '=='
	#print(str(op1) + " " + str(con1))

	op2 = ""
	con2 = ""
	if len(conditions) > 1:						#if 2nd condition exists
		op2 = findOperator(conditions[1])		#find operator present in condition2
		con2 = conditions[1].split(op2)			#seaprate operands
		con2 = [x.strip() for x in con2]		#remove whitespaces
		if op2 == '=':	#convert SQL '=' to '=='
			op2 = '=='
		#print(str(op2) +  " " + str(con2))

	
	#print(conditions)
	
	try:
		res = []
		for row in product_table:
			i1 = product_cols.index(con1[0])	#col index for 1st condition
			i2 = -1								#col index for 2nd condition
			if len(conditions) > 1:	
				i2 = product_cols.index(con2[0])


			if len(conditions) == 1  :		#only 1 condition 
				#2nd operand is an integer digit
				if isInt(con1[1]):
					if eval(str(row[i1]) + op1 + con1[1]): 
						res.append(row)
				else:				#when 2nd opearand is column name
					j1 = product_cols.index(con1[1])
					if eval(str(row[i1]) + op1 + str(row[j1])):
						res.append(row)

			elif len(conditions) == 2 :
				#1st one col-int 2nd one col-int
				if isInt(con1[1]) and isInt(con2[1]):
					if eval(str(row[i1]) + op1 + con1[1] + " " + andor + " " + str(row[i2]) + op2 + con2[1]):
						res.append(row)

				#1st one col-col 2nd one col-int
				elif not isInt(con1[1]) and isInt(con2[1]):
					j1 = product_cols.index(con1[1])
					if eval(str(row[i1]) + op1 + str(row[j1]) + " " + andor + " " + str(row[i2]) + op2 + con2[1]):
						res.append(row)

				#1st one col-int 2nd one col-col
				elif isInt(con1[1]) and not isInt(con2[1]):
					j2 = product_cols.index(con2[1])
					if eval(str(row[i1]) + op1 + con1[1] + " " + andor + " " + str(row[i2]) + op2 + str(row[j2])):
						res.append(row) 

				#1st one col-col 2nd one col-col
				elif not isInt(con1[1]) and not isInt(con2[1]):
					j1 = product_cols.index(con1[1])
					j2 = product_cols.index(con2[1])
					if eval(str(row[i1]) + op1 + str(row[j1]) + " " + andor + " " + str(row[i2]) + op2 + str(row[j2])):
						res.append(row)
	
	except:
		print("invalid where condition")
		exit()
	

	#print(res)
	return res


#####################################################################################
def maxagg(col_i,product_table):
	temp = []
	for x in product_table:
		temp.append(int(x[col_i]))
	return max(temp)
#####################################################################################
def minagg(col_i,product_table):
	temp = []
	for x in product_table:
		temp.append(int(x[col_i]))
	return min(temp)
#####################################################################################
#def countagg(col_i,product_table):
#	return len(product_table)
#####################################################################################
def sumagg(col_i,product_table):
	temp = 0
	for x in product_table:
		temp = temp + (int(x[col_i]))
	return temp
#####################################################################################
def avgagg(col_i,product_table):
	temp = 0
	for x in product_table:
		temp = temp + (int(x[col_i]))
	return float(temp)/len(product_table)
#####################################################################################
def onlyAgregate(product_table , product_cols  , output_columns):

	#only agregate function is there
	aggfunc = output_columns[0]
	aggfunc = aggfunc.replace('(',' ')	#'max(A)'' --> 'max A)'
	aggfunc = aggfunc.replace(')','')	#'max(A)'' --> 'max A'
	aggfunc = aggfunc.split()			#['max','A']
	aggfunc = [x.strip() for x in aggfunc]	#remove spaces if any
	#print(aggfunc)

	val = 0
	if aggfunc[0].lower() == "max":
		val = maxagg(product_cols.index(aggfunc[1]),product_table)
		product_table = [[val]]		#list of list is table
		product_cols = [output_columns[0]]

	elif aggfunc[0].lower() == "min":
		val = minagg(product_cols.index(aggfunc[1]),product_table)
		product_table = [[val]]		#list of list is table
		product_cols = [output_columns[0]]

	elif aggfunc[0].lower() == "count":
		val = len(product_table)
		product_table = [[val]]		#list of list is table
		product_cols = [output_columns[0]]

	elif aggfunc[0].lower() == "sum":
		val = sumagg(product_cols.index(aggfunc[1]),product_table)
		product_table = [[val]]		#list of list is table
		product_cols = [output_columns[0]]

	elif aggfunc[0].lower() == "average":
		val = avgagg(product_cols.index(aggfunc[1]),product_table)
		product_table = [[val]]		#list of list is table
		product_cols = [output_columns[0]]
		
	#print(val)
	return product_table , product_cols


#####################################################################################
def groupBy(grpby_col , product_table , product_cols  , output_columns):
	i = product_cols.index(grpby_col)	#index of grp by col in prod table
	groups = {}

	#break table according to grp by column
	for row in product_table:
		if row[i] not in groups:
  			groups[row[i]] = []  
		groups[row[i]].append(row)

	#print groups
	#print(product_cols)
	#for x in groups:
	#	for t in groups[x]:
	#		print(t)
	#	print()

	res = []
	for group in groups:
		group = groups[group]	#group is the key but we need values here
		temp = []

		for col in output_columns:
			if col.find('(') != -1 and col.find(')') != -1:	#agregate function
				aggfunc = col
				aggfunc = aggfunc.replace('(',' ')	#'max(A)'' --> 'max A)'
				aggfunc = aggfunc.replace(')','')	#'max(A)'' --> 'max A'
				aggfunc = aggfunc.split()			#'max A' --> ['max','A']
				aggfunc = [x.strip() for x in aggfunc]	#remove spaces if any

				val = 0
				if aggfunc[0].lower() == "max":
					val = maxagg(product_cols.index(aggfunc[1]),group)	#product col are same as grp col
					temp.append(val)

				if aggfunc[0].lower() == "min":
					val = minagg(product_cols.index(aggfunc[1]),group)	#product col are same as grp col
					temp.append(val)

				if aggfunc[0].lower() == "count":
					val = len(group)			#number of rows in grp
					temp.append(val)

				if aggfunc[0].lower() == "sum":
					val = sumagg(product_cols.index(aggfunc[1]),group)	#product col are same as grp col
					temp.append(val)

				if aggfunc[0].lower() == "average":
					val = avgagg(product_cols.index(aggfunc[1]),group)	#product col are same as grp col
					temp.append(val)		
		
			else:
				temp.append(group[0][product_cols.index(col)])
				#from first row of grp pick value of given column
				#all values in that col will be same as grp by is used on that col

		res.append(temp)

	return res


#####################################################################################
def executeQuery(query , metadata , output_columns , query_tables ):
	product_table , product_cols = getCrossProduct(query_tables,metadata)			#retrieve cross product table 
	
	#print table
	'''																				#and respective column names
	print("cross product is:\n")
	print(product_cols)
	for row in product_table:
		print(row)
	print("total rows = " + str(len(product_table)) + "\n")
	'''


	#where clause
	for token in query:
		if re.search("^where", token, re.IGNORECASE):
			token = token.replace("where","").strip()
			product_table = whereConditon(token , product_table , product_cols)
			break


	grpby = False
	for token in query:
		if re.search("^group", token, re.IGNORECASE):		#query has 'group by'
			#print("group by used")
			grpby = True
			try:
				grpby_col = query[query.index(token) + 1]
				product_table = groupBy(grpby_col , product_table , product_cols  , output_columns)
				product_cols = output_columns
			except:
				print("invalid grp by column")
				exit()

	#Agregate function but no grp by
	if not grpby:
		for col in output_columns:
			if col.find('(') != -1 and col.find(')') != -1:	#agregate function have '()'
				product_table , product_cols = onlyAgregate( product_table , product_cols  , output_columns)
				#print(str(product_cols) + " " + str(output_columns))


	if query[1].lower() == "distinct" :
		product_table  = distinctCols(product_table , product_cols ,  output_columns)
		if output_columns[0]!="*":
			product_cols = output_columns		


	for token in query:
		if re.search("^order", token, re.IGNORECASE):
			order_params = query[query.index(token) + 1]	#column name and order of sorting
			#print(order_params)			
			product_table = orderBy(product_table,product_cols,order_params)

	
	
	#Handling select *.. OR select A,B... clause
	print_op_cols(product_table , product_cols , output_columns , metadata)		#to print required output columns


#####################################################################################
def main():

	#make query LOWER case
    query1 = 'select * from table4,table3 where A>=3 or D=8 order by D desc group by Y'
    query2 = 'select distinct abcd,count(efgh) from foo where abc = 123 and def=124 group by abcd order by efgh asc'
    query3 = 'select distinct * from table3, table4 order by D '
    query4 = 'select sum(A) from table4,table3 where A>2'
    query5 = 'select A,avg(D),max(B),min(C),sum(E),count(*) from table3,table4 group by A'
    #agregate with distinct not working
    query = sys.argv[1]
    query = query.replace(";","")
    #print()
    #print(query)

    parsed = sqlparse.parse(query)[0].tokens
    temp = sqlparse.sql.IdentifierList(parsed).get_identifiers()

    tokenized = []		#store query after breaking by identifiers
    for i in temp:
    	tokenized.append(str(i))
    #print("tokenized query: " + str(tokenized) + "\n")


    i=0
    while i<len(tokenized):
    	if re.search("^from", tokenized[i], re.IGNORECASE):
    		break
    	i+=1
    #query_tables = tokenized.index("from")+1;	#table names start just  after 'from'
    query_tables = tokenized[i+1]
    query_tables = [x.strip() for x in query_tables.split(',')]
    #print("tables in query: " + str(query_tables) + "\n")

    
    output_columns = []
    if tokenized[1].lower() == "distinct" :
    	output_columns = tokenized[2]    	
    else:
    	output_columns = tokenized[1]
    output_columns = [x.strip() for x in output_columns.split(',')]		#remove space
    #print("columns to print: " + str(output_columns) + "\n")
	

    metadata = parseMetaData()		#resolve metadata tablename->[col names]
    #print("metadata: " + str(metadata) +"\n")

    executeQuery(tokenized,metadata,output_columns,query_tables)


#####################################################################################
if __name__ == "__main__":
	try:

		main()
	except:
		print("invalid query")
		exit()