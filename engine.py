import re
import sys
import pickle
import sqlparse
import os
from collections import OrderedDict
import numpy as np


database = {}
# join = ""


def get_schema():
    database = {}
    with open('./metadata.txt', 'r') as f:
        tableName = ''
        tableAttr = ''
        for line in f:
            if '<begin_table>' in line:
                table = 1
            elif '<end_table>' in line:
                table = 0
            else:
                if table == 1:
                    # print(line+'0')
                    # tableName = line.replace('\r\n', '')
                    tableName = line[0:len(line)-1]
                    # print(tableName+'0')
                    database[tableName] = {}
                    database[tableName]['records'] = []
                    database[tableName]['schema'] = []
                    table = 2
                else:
                    # tableAttr = line.replace('\r\n', '')
                    tableAttr = line[0:len(line)-1]
                    database[tableName]['schema'].append(tableAttr)
    return database

def check(database):
    for table in database.keys():
        # print(table)
        # print(database[table]['records'])
        sum = 0
        for record in database[table]:
            l = len(record)
            print(record)
            sum = sum + l
        # print(sum)

def fill(database):
    for table in database.keys():
        tab = str(table)
        # print(len(tab))
        # tab = tab[0:len(tab)-1]
        # print(len(tab))
        with open('./'+tab+'.csv') as f:
            for line in f:
                values = [float(value) for value in line.split(',') if not value == '']
                # for v in values:
                #     print(type(v))
                database[table]['records'].append(tuple(values))
    return database


def queryParser(query,statement):
    # print(query)
    att = []
    tables = []
    keywords = []
    cond = None



    if "FROM" in query or "from" in query:
        query = query.lower()
        obj1 = query.split('from')
    else:
        print("Invalid Syntax")
        # exit(0)

    # print(obj1)

    if "select" not in obj1[0]:
        print("Incorrect Syntax")
        exit(0)



    # return 1,2,3,4
    # print(query," - query")
    # query = query.replace(';','')
    # statement = sqlparse(query)[0]
    # print(statement," -statement")
    flag = 0
    for token in statement.tokens:

        if(str(token.ttype) == 'Token.Keyword.DML' or str(token.ttype) == 'Token.Keyword'):
            keywords.append(str(token))

        if(flag==0 and (str(token.ttype) == 'None' or str(token.ttype) == 'Token.Wildcard')):
            str_att = re.sub(' +',' ',str(token.value))
            att = str_att.replace(' ', '').split(',')
            flag = flag +1

        elif(flag==1 and str(token.ttype) == 'None'):
            str_tab = re.sub(' +',' ',str(token.value))
            tables = str_tab.replace(' ', '').split(',')
            flag = flag +1

        elif(flag==2 and str(token.ttype) == 'None'):
            cond = str(token).replace('WHERE ','')
            flag=flag+1

    if(att == [] or tables == []):
        print("Invalid Syntax")
        exit(0)

    # print(att)
    # print(tables)
    # print(keywords)
    # print(cond)
    return att,tables,keywords,cond

def existence(databse,att,tables):
        ex = {}

        # for table in tables:
        #     for a in database[table]['schema']:
        #         print(a)
        # return 1

        for table in tables:
            try:
                for a in database[table]['schema']:
                    # print(str(a)," - in table ",str(table))
                    try:
                        ex[str(a)].append(str(table))
                    except KeyError:
                        ex[str(a)] = [str(table)]
                    try:
                        ex[str(table)+'.'+str(a)].append(str(table))
                    except KeyError:
                        ex[str(table)+'.'+str(a)] = [str(table)]
            except KeyError:
                print(str(table)," does not exist")
                exit(0)
        return ex

def conditional(table,query,existence):
    if(query==None):
        return table

    query = query.replace('AND','and')
    query = query.replace('OR','or')
    query = query.replace('<=','!')
    query = query.replace('>=','?')
    join = ""
    # lol = "AND"
    operators = ['=','<','>','?','!']
    # print(query)
    conditionals = query.replace('(', '').replace(')', '').replace('and', '^').replace('or', '^')
    conditionals = conditionals.split('^')
    # print(conditionals)

    i = 0
    records = table['records']
    bitmap = np.zeros((len(records), ), dtype=float)
    join_idx = -2
    for record in records:
        temp_query = query
        for condi in conditionals:
            found = None
            condi = condi.strip()
            for op in operators:
                if op in condi:
                    found = op
                    [attribute,value] = condi.split(op)
                    value = (value.strip())
                    value = value.replace(';','')
                    attribute = attribute.strip()
                    break
            # print(value)
            # if("table" not in value):
            #     value = int(value)
            # print(type(value))

            # if("table" in attribute and "table" in value):
            #     join = value
                # print(join+ " -joined at")
            if found == None:
                print("Operations defined are =,>,< only")
                exit(0)
            else:
                try:
                    if(len(existence[attribute]) >1):
                        print(str(attribute)," is present in more than one table")
                        exit(0)

                    if( '.' in attribute):
                        index = table['schema'].index(attribute)
                    else:
                        index = table['schema'].index(str(existence[attribute][0])+'.'+str(attribute))

                except KeyError:
                    print(str(attribute)," not found in any table 1")
                    exit(0)

                if( value in existence.keys()):
                    if(len(existence[value]) > 1):
                        print(str(value)," is present in more than one table")
                        exit(0)

                    if '.' in value:
                        join_att = value
                    else:
                        join_att = str(existence[value][0])+'.'+str(value)

                    join_idx = table['schema'].index(join_att)

                    if found == "=":
                        temp_query = temp_query.replace(condi,str(int(record[index] == record[join_idx])))

                    elif found == "<":
                        temp_query = temp_query.replace(condi,str(int(record[index] < record[join_idx])))

                    elif found == ">":
                        temp_query = temp_query.replace(condi,str(int(record[index] > record[join_idx])))

                    elif found == "?":
                        temp_query = temp_query.replace(condi,str(int(record[index] >= record[join_idx])))

                    elif found == "!":
                        temp_query = temp_query.replace(condi,str(int(record[index] <= record[join_idx])))
                else:

                    try:
                        if found == "=":
                            temp_query = temp_query.replace(condi,str(int(records[i][index] == float(value))))

                        elif found == "<":
                            temp_query = temp_query.replace(condi,str(int(records[i][index] < float(value))))

                        elif found == ">":
                            temp_query = temp_query.replace(condi,str(int(records[i][index] > float(value))))

                        elif found == "?":
                            temp_query = temp_query.replace(condi,str(int(records[i][index] >= float(value))))

                        elif found == "!":
                            temp_query = temp_query.replace(condi,str(int(records[i][index] <= float(value))))

                    except:
                        print(str(value)+" is an unknown var")
                        exit(0)
        # print(temp_query)
        bitmap[i] = eval(temp_query)
        i = i+ 1
    records = table['records']
    # print(records[2][2])
    # records.pop(join_idx)
    # print(join_idx)
    if(join_idx > 0 and found == '='):
        records[join_idx] = ""
    conditionalTable = {}
    conditionalTable['records'] = []
    # print(bitmap.shape)
    conditionalTable['schema'] = table['schema']
    # print((conditionalTable['schema']))
    if(join_idx > 0 and found == '='):
        conditionalTable['schema'].pop(join_idx)
    for i in range(bitmap.shape[0]):
        x = []
        if bitmap[i] >=1:
            for j in range(len(records[i])):
                if(j == join_idx and found == '='):
                    continue
                else:
                    x.append(records[i][j])
            conditionalTable['records'].append(x)
    join_idx = -2
    # print(conditionalTable)
    return conditionalTable


def cross_join(tables, tablesYet, newTable,database):
    if len(tables) == tablesYet:
        return

    table = tables[tablesYet]
    for attr in database[table]['schema']:
        if not str(table)+'.' in attr:
            # print(str(table)+'.'+str(attr))
            newTable['schema'].extend([str(table)+'.'+str(attr)])

    if len(newTable['records']) == 0:
        newTable['records'] = database[tables[tablesYet]]['records']
    else:
        x = []
        for record in database[tables[tablesYet]]['records']:
            x.extend([each_record+record for each_record in newTable['records']])
        newTable['records'] = x
    cross_join(tables, tablesYet+1, newTable,database)

    return newTable

def show(table,att):

    if('*' in att):
        att = table['schema']

    indices = []
    sc = "^"
    for i in range(len(table['schema'])):
        for attr in att:
            if(str(attr) in str(table['schema'][i])):
                sc = sc+','+str(table['schema'][i])
                indices.append(i)
    # print(att)
    sc = sc.replace('^,','')
    print(sc)
    for l in range(len(table['records'])):
        # for i in indices:
        #     print(table['records'][l][i])
        print(','.join([str(table['records'][l][i]) for i in indices]))
            # print([str(val) for val in table['records'][l][0]])

def projectedTable(table, att,keywords,existenceTable):
    for attr in att:
        temp = re.sub(' +','',attr).replace(')','')

        if "min" in temp:
            temp = temp.replace('min(','')
            # print(temp)
            try:

                if(len(existenceTable[str(temp)])>1):
                    print(str(temp)+" is present in more than one table")
                    exit(0)
                if(len(existenceTable[str(temp)]) < 1):
                    print(str(temp)+" is not present in any db table")
                    exit(0)


                # print(existenceTable[temp][0]," ",temp)
                if '.' in temp:
                    index = table['schema'].index(temp)
                else:
                    index = table['schema'].index(str(existenceTable[temp][0])+'.'+str(temp))
                print('MIN('+str(temp)+'):')

                if not table['records']==[]:
                    ans = table['records'][0][index]
                    for record in table['records']:
                        ans = min(ans, record[index])
                    print(ans)
            except KeyError:
                print(str(temp)+' not found in table')
            sys.exit()

        elif "max" in temp:
            temp = temp.replace('max(','')
            try:

                if(len(existenceTable[str(temp)])>1):
                    print(str(temp)+" is present in more than one table")
                    exit(0)
                if(len(existenceTable[str(temp)]) < 1):
                    print(str(temp)+" is not present in any db table")
                    exit(0)


                if '.' in temp:
                    index = table['schema'].index(temp)
                else:
                    print(table['schema'].index(str(existenceTable[temp][0])+'.'+str(temp)))
                    index = table['schema'].index(str(existenceTable[temp][0])+'.'+str(temp))
                print('MAX('+str(temp)+'):')

                if not table['records']==[]:
                    ans = table['records'][0][index]
                    for record in table['records']:
                        ans = max(ans, record[index])
                    print(ans)
            except KeyError:
                print(str(temp)+' not found in table')
            sys.exit()

        elif 'sum' in temp:
            temp = temp.replace('sum(', '')
            try:
                if len(existenceTable[str(temp)]) > 1:
                    print(str(temp)+' is present in more than one table')
                    sys.exit()
                if len(existenceTable[str(temp)]) < 1:
                    print(str(temp)+' is not present in any database table')
                    sys.exit()


                ans = 0
                if '.' in temp:
                    index = table['schema'].index(temp)
                else:
                    index = table['schema'].index(str(existenceTable[temp][0])+'.'+str(temp))
                print('SUM('+str(temp)+'):')
                if not table['records'] == []:
                    for record in table['records']:
                        ans += record[index]
                    print(ans)
            except KeyError:
                print(str(temp)+' not found in table')
            sys.exit()

        elif 'avg' in temp:

            temp = temp.replace('avg(', '')
            try:
                if len(existenceTable[str(temp)]) > 1:
                    print(str(temp)+' is present in more than one table')
                    sys.exit()
                if len(existenceTable[str(temp)]) < 1:
                    print(str(temp)+' is not present in any database table')
                    sys.exit()

                ans = 0
                if '.' in temp:
                    index = table['schema'].index(temp)
                else:
                    index = table['schema'].index(str(existenceTable[temp][0])+'.'+str(temp))
                print('AVG('+str(temp)+'):')
                if not table['records'] == []:
                    for record in table['records']:
                        ans += record[index]
                    print((ans*1.0) / len(table['records']))
            except KeyError:
                print(str(temp)+' not found in table')
            sys.exit()

    indices = []

    if not '*' in att:
        for attr in att:
            try:
                # print(existenceTable[str(attr)])
                if(len(existenceTable[str(attr)]) >1):
                    print(str(attr)+" is present in more than one table")
                    exit(0)
                if len(existenceTable[str(attr)]) < 1:
                    print(str(attr)+' is not present in any database table')
                    exit(0)

            except KeyError:
                print(str(attr)+' not present in database')
                sys.exit()

            # print(table['schema'])

        # for attr in att:
        #     if '.' in attr:
        #         indices.append(table['schema'].index(attr))
        #     else:
        #         indices.append(table['schema'].index(str(existenceTable[attr][0])+'.'+str(attr)))
            try:
                # print(attr)
                # if(join == attr):
                #     continue
                if '.' in attr:
                    indices.append(table['schema'].index(attr))
                else:
                    indices.append(table['schema'].index(str(existenceTable[attr][0])+'.'+str(attr)))
            except ValueError:
                print(str(attr)+' attribute not found in any table')
                sys.exit()

        # print(indices)
        # for i in indices:
        #     # print(table['schema'][i])
        #     # print(join)
        #     if(table['schema'][i] == join):
        #         indices.remove(i)

        # print(indices)

        temp = []
        for record in table['records']:
            temp.append(tuple([record[i] for i in indices]))

        #
        # table['schema'] = [attr for attr in att]
        table['schema'] = [table['schema'][i] for i in indices]

        if 'DISTINCT' in keywords:
            table['records'] = set(temp)
        else:
            table['records'] = temp
    return table


def showTable(table):
    # print('\noutput:')
    print(','.join([str(attribute) for attribute in table['schema']]))
    for record in table['records']:
        print(','.join([str(value) for value in record]))


if __name__ == '__main__':
    # print("ok")

    if(len(sys.argv) < 2):
        print("Pls give sql thing to command line u ho\n")

    else:
        # print(sys.argv[8])
        # s = str(sys.argv[1])
        # print(len(sys.argv))
        s = ''
        for x in range(1,len(sys.argv)):
            s = s+ sys.argv[x]+' '
        query = sqlparse.parse(s)[0]
        # query_tokens = query.tokens
        # print(query)
        # print(query_tokens)
        db = get_schema()
        database = fill(db)
        # print(s)
        # check(database)

        att, tables, keywords, cond = queryParser(s,query)
        existenceTable = existence(database,att,tables)
        # print("ex")
        # print(existenceTable)
        # for val in existenceTable:
        #     print(val)

        newTable = {}
        newTable['schema'] = []
        newTable['records'] = []

        fullTables = cross_join(tables,0,newTable,database)
        # print(len(fullTables))
        # print(type(database),type(fullTables))
        # check(fullTables)
        if(cond == None):
            # print(att)
            count = 0
            for attr in att:
                if ('max' in attr) or ('avg' in attr) or ('sum' in attr) or ('min' in attr):
                    count = 1
            # print("ghusa")
            if(count == 0):
                show(fullTables,att)
                exit(0)
        #


        cond_table= conditional(fullTables,cond,existenceTable)
        proj = projectedTable(cond_table,att,keywords,existenceTable)
        showTable(proj)
