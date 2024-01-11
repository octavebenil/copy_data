import psycopg2 as psycopg2


def copy_update_data():
    #seelect data from table source and copy to another database
    #use psycopg2
    pg_dest = psycopg2.connect(host="localhost", database="database", user="postgres", password="pass")
    pg_source = psycopg2.connect(host="localhost", database="databa", user="postgres", password="pass2")


    cursor_source = pg_source.cursor()
    cursor_dest = pg_dest.cursor()

    table_to_copy = [

    ]

    dest_table = [

    ]


    for table in table_to_copy:
        #copy all data in table to destination tables dest_table

        #get column names source table
        cursor_source.execute("select column_name from information_schema.columns where table_name = '" + table + "'")
        columns = cursor_source.fetchall()

        #get index of column code
        code_index = None
        for column in columns:
            if column[0] == 'code':
                code_index = columns.index(column)
                break
        cursor_source.execute("select * from "+table)
        rows = cursor_source.fetchall()
        for row in rows:
            #where can content ' or " in code
            #escape character ' in code
            code_r = str(row[code_index])
            print(code_r)
            code_r = code_r.replace("'", "\\'")
            print(code_r)

            cursor_dest.execute("select * from " + dest_table[table_to_copy.index(table)] + " where code = E'" + code_r + "'")

            if cursor_dest.fetchone() is None:
                prepared_query_columns = "("
                prepared_query_values = "("

                for column in columns:
                    prepared_query_columns = prepared_query_columns + column[0] + ","
                    prepared_query_values = prepared_query_values + "%s,"

                prepared_query_columns = prepared_query_columns[:-1] + ")"
                prepared_query_values = prepared_query_values[:-1] + ")"

                prepared_query = "insert into " + dest_table[table_to_copy.index(table)] + prepared_query_columns + " values " + prepared_query_values

                print(prepared_query)

                cursor_dest.execute(prepared_query, row)
                pg_dest.commit()

if __name__ == '__main__':
    copy_update_data()
