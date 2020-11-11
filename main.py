import codecs

import mysql.connector as mysql
from mysql.connector import Error

import string
import os

host = '127.0.0.1'
port = '3306'
user = 'root'
password = 'matteo.ciniselli'
database = "usi"
ssl_disabled = True

query = ""
is_insert_query = True

"""

create table test (
	id int NOT NULL AUTO_INCREMENT,
	field1 int,
    field2 varchar(255),
    field3 varchar(255),
    PRIMARY KEY(id)
    )
    
"""
###########################
# UTILITIES               #
###########################

'''
return a connection object
'''
def get_connection():
    return mysql.connect(
        host=host,
        port=port,
        user=user,
        passwd=password,
        database=database
    )

def read_file(filepath):  # read generic file
    try:
        with open(filepath, encoding="utf-8") as f:
            content = f.readlines()
        c_ = list()
        for c in content:
            r = c.rstrip("\n").rstrip("\r")
            c_.append(r)

    except Exception as e:
        print("Error ReadFile: " + str(e))
        c_ = []
    return c_


def write_file(filename, list_item):  # write generic file
    file = codecs.open(filename, "w", "utf-8")

    for line in list_item:
        file.write(line + "\n")

    file.close()


###########################
# INSERT QUERY            #
###########################

'''
return the query
table is the table in which you want to insert the data
fields are a list of all fields
values are a list of all values (if you want to insert more than one record you can pass a list of n*len(fields)
is_string is a list of boolean (True if the fields in that position is a string, False otherwise
'''
def create_insert_query(table, fields, values, is_string):
    fields_cleaned = list()
    for f in fields:
        fields_cleaned.append(f.replace("\\", "\\\\").replace("'", "\\'"))

    values_cleaned = list()

    if len(values) % len(fields) != 0:
        print("ERROR: number of values is not multiple of number of fields")
        return ""

    num_times = len(values) // len(fields)

    is_string_same_length = list()

    for i in range(num_times):
        is_string_same_length.extend(is_string)

    for f, s in zip(values, is_string_same_length):
        value_curr = f.replace("\\", "\\\\").replace("'", "\\'")
        if s == True:
            value_curr = "'{}'".format(value_curr)
        values_cleaned.append(value_curr)

    if len(fields_cleaned) == len(values_cleaned):
        list_of_fields = ", ".join(fields_cleaned)
        list_of_values = ", ".join(values_cleaned)

        query = "INSERT INTO {} ({}) VALUES  ({});".format(table, list_of_fields, list_of_values)
        return query
    else:
        list_of_fields = ", ".join(fields_cleaned)
        list_insert = list()
        for i in range(num_times):
            list_temp = values_cleaned[i * len(fields):(i + 1) * len(fields)]
            list_insert.append("({})".format(", ".join(list_temp)))

        values_insert = ", ".join(list_insert)

        query = "INSERT INTO {} ({}) VALUES {};".format(table, list_of_fields, values_insert)
        return query

'''
given a query this function executes the query and returns the result
'''

def insert_query(query):
    conn = None
    try:
        conn = get_connection()
    except Exception as e:
        print("ABB")
        print(e)

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

    except mysql.Error as e:
        print(e)
        print(f'store_commits() query exception: {type(e).__name__} {e.args}')

    if (conn.is_connected()):
        cursor.close()
        conn.close()

'''
You can call this function to create and run the query
'''
def insert_in_database(table, fields, values, is_string):
    query = create_insert_query(table, fields, values, is_string)
    insert_query(query)


###########################
# SELECT QUERY            #
###########################

'''
Read data from database
you can run two different kind of queries:
1.
you can set a field with start and end to filter data with the following where condition
WHERE field >= start and field < end
2.
You can set field and values if you want to filter data with the following where condition
WHERE field IN (value1, value2, value3)

You can set also a step to set the maximum number of records to retrieve in each query

'''

def read_from_data(table, field, start, end, step, values):
    where_in = False
    if values is not None:
        where_in = True

    connection = get_connection()

    if where_in:
        values_noduplicates = list(dict.fromkeys(values))

        num_chunks = len(values_noduplicates) // step

        if len(values_noduplicates) % step != 0:
            num_chunks += 1

        result_global = list()

        for i in range(num_chunks):
            data = values_noduplicates[i * step: (i + 1) * step]
            result = get_chunk_of_data_where_in(table, field, data, connection)
            if len(result) > 0:
                result_global.extend(result)

    else:

        num_chunks = int(end - start) // step

        if int(end - start) % step != 0:
            num_chunks += 1

        result_global = list()

        for i in range(num_chunks):
            start_curr = start + i * step

            end_curr = start + (i + 1) * step
            if end_curr > end:
                end_curr = end

            result = get_chunk_of_data(table, field, start_curr, end_curr, connection)

            if len(result) > 0:
                result_global.extend(result)

    if connection.is_connected():
        connection.close()

    return result_global

'''
this function is called from read_from_data when you want to retrieve data using first where condition
'''

def get_chunk_of_data(table, field, start, end, connection):  # read data from method folder
    result = list()
    try:

        connection = get_connection()

        sql_select_Query = "select * from {} where {}>={} and {} < {} ".format(table, field, start, field, end)
        print(sql_select_Query)
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()

        print(len(records))

        for row in records:
            row_temp = str(row)
            # if there are some not printable characters, we skip the line
            bb = ''
            for char in row_temp:
                if char in string.printable:
                    bb += char
            if len(bb) == len(row_temp):
                result.append(row)
            else:
                print("ROW SKIPPED: {}".format(row))

    except Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        if (connection.is_connected()):
            cursor.close()
    return result

'''
this function is called from read_from_data when you want to retrieve data using second where condition
'''
def get_chunk_of_data_where_in(table, field, values,
                               connection):  # read data from method folder (with clause where field in ()
    result = list()

    try:

        field_value = ", ".join(values)

        sql_select_Query = "select * from {} where {} IN ( {} ) ".format(table, field, field_value)

        print("QUERY: {}".format(sql_select_Query))
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()

        for row in records:
            row_temp = str(row)
            # if there are some not printable characters, we skip the line
            bb = ''
            for char in row_temp:
                if char in string.printable:
                    bb += char
            if len(bb) == len(row_temp):
                result.append(row)
            else:
                print("ROW SKIPPED: {}".format(row))

    except Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        if (connection.is_connected()):
            cursor.close()

    return result

###########################
# EXPORT FUNCTION         #
###########################
'''
this function is used to export data.
if you set a separator you will store all records in record.txt files. Each row in made up of all values, separated by the separator string
separator="|||"
record=(1,2,3)
result="1|||2|||3"

if you set export_in_different_files all records will be saved in different files (one for each fields). The name of the file will be 0.txt, 1.txt, ...
'''

def export_data(records, output_folder, export_in_different_files, separator):
    if os.path.exists(output_folder) == False:
        os.makedirs(output_folder)

    if separator is not None:
        result = list()

        for r in records:
            list_t = list(r)
            print(type(list_t))
            print(list_t)
            row = separator.join([str(e) for e in list_t])
            result.append(row)

        write_file(os.path.join(output_folder, "result.txt"), result)

    elif export_in_different_files is not None:
        num_fields = len(records[0])
        for i in range(num_fields):
            result = list()
            for rec in records:
                result.append(str(rec[i]))

            write_file(os.path.join(output_folder, "{}.txt".format(i)), result)


###########################
# TEST CASES              #
###########################

def test_insert():
    table = "test"
    fields = ["field1", "field2", "field3"]
    values = ["11", "22", "33", "44", "55", "66"]
    is_string = [False, True, True]

    insert_in_database(table, fields, values, is_string)


def test_select():
    table = "test"
    field = "field1"
    start = 10
    end = 50
    step = 15
    values = None
    r = read_from_data(table, field, start, end, step, values)
    return r


def test_select2():
    table = "test"
    field = "field1"
    start = 10
    end = 50
    step = 1
    values = ["11", "22"]
    r = read_from_data(table, field, start, end, step, values)
    return r


def test_export_single_file():
    output_folder = "output"
    records = test_select()
    export_data(records, output_folder, None, "||_||")


def test_export_all_fields():
    output_folder = "output"
    records = test_select()
    export_data(records, output_folder, True, None)


def main():
    # test_insert()
    # test_select2()
    # test_export_single_file()
    test_export_all_fields()


if __name__ == "__main__":
    main()
