import sqlite3
import pickle
import os
import json
import enum
from itertools import *
class ColumnData(enum.Enum):
    cid = 0
    name = 1
    type = 2
    notnull = 3
    dflt_value = 4
    pk = 5


def handle_path(path):
    valid_path = '/'.join(list(filter(lambda x: len(x) > 0, path.replace('\\', '/').split("/"))))
    # print(valid_path)
    # print(os.path.isfile(valid_path))
    return valid_path


def remove_space(str_input):
    return str_input.replace(" ", "").replace("\n", "")


def find_with_different_cases(text, find_arg):
    return (text.lower()).index(find_arg.lower())


def create_folder(folder_dir):
    folder_path = handle_path(folder_dir)
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)
#Needs to be a private function that the user can't activate

main_directory = handle_path(os.getcwd())
database_dir = main_directory + '/database'

# db_con = sqlite3.connect(database_dir + '/Main_Server_database.db')
# db_cursor = db_con.cursor()


class WritingError(Exception):
    """
    When the user enters values that contradict
    """
    pass


"""
    "type code" = the code that will be written in the querry to determine the type of the column
    limit size = 1
    dumping_function = 2
    loading_function = 3
"""


#Get rid of it!
data_types_encode = {
    int: {'type code': ['int','integer']},
    str: {'type code': ['varchar']},
    float: {'type code': ['float']},
    bytes: {'type code': ['blob']},
    type(None): {'type code': ['null']}
}


def add_handler(new_type, type_codes, dumping_function, loading_function):
    """
    If the user wants
    :param new_type: can be an object or a new type that is recognized by the
    :param type_codes:
    :param dumping_function:
    :param loading_function:
    :return:
    """
    data_types_encode[new_type]

# I Convert a list and a dictionary to a string representation so it can be written in the database.

# for type_value in data_types_encode.values():
#    type_value['type code'] = type_value['type code'] + '({})'.format(type_value['limit size'])

class Database:
    """
    This class handles a database, reads from it information and
    """
    __add_args_name = "add_args"
    __encoded_data_type = "longblob"
    # This module will automaicly decode anything in columns of data type "longblob" by unpickling.
    # This is how the module saves data types python supports but SQL doesn't like lists and dictionaries:
    # It pickles them.
    # SO the user MUST saves dictionaries and lists and such under the data type "longblob".

    def __init__(self, db_name, db_loc=os.getcwd()):
        self.db_name = db_name
        if db_loc == 'memory':
            self.db_con = sqlite3.connect(':memory:')
            self.db_loc = None
        else:
            self.db_loc = handle_path(db_loc)
            self.db_folder = self.db_loc + '/database'
            create_folder(self.db_folder)
            self.db_file_dir = self.db_folder + '/' + self.db_name
            if not self.db_file_dir.endswith('.db'):
                self.db_file_dir += '.db'
            self.db_con = sqlite3.connect(self.db_file_dir)

        self.db_cursor = self.db_con.cursor()

    def execute_sql_code(self, sql_code, code_args=None, encrypt_args=None):
        """
            Executes a code that changes the database, and saves the changes.
            It is important to know that when the program "dumps" data into a database, it encrypts data types SQL
            doesn't support by pickling them and saves them as bytes.
            :param sql_code: a string of a sql code.
            :param code_args: a dictionary/list of values you want to safely escape values into the code.
            :param encrypt_args: a dictionary/list of values you want to safely escape values into the code that
            are data types SQL doesn't support: what isn't ints, doubles or floats, strings and bytes.
            Data types like lists, dictionaries and such.
            It is important if the condition in your code checks values like these, it won't find them.
            So it is best to put them here.
            Note that the program automatically encrypts values that are not in the "code_args" or "encrypt_args",
            like the values you "dump" or update your database with.
            Note also that if the "code_args" and "encrypt_args" are of different data types, a dictionary o a list,
            it will unify them.

            If it's a dictionary, you escape values, by writing a certain string(a name), with a ":" before it
            in the code where you want to insert the value. Then in the dictionary, pair the value to that string
            (without the ":").
            If it's a list, then write "?" in the code where you want to insert the value.

            If you have in your code "?" and ":" and you want to escape both a list and a dictionary,
            then add to the dictionary a key "add_args"(for "additional arguments"), that is paired with the list.

        """
        with self.db_con:
            try:
                if code_args:
                    self.db_cursor.execute(sql_code, code_args)
                else:
                    self.db_cursor.execute(sql_code)

                self.db_con.commit()
            except sqlite3.Error as e:
                print(e)
                # raise e

    def collect_sql_quarry_result(self, sql_code, quarry_args=None, num_of_rows=None, filer_unique_row=True):
        """
        Asks for information from the database. It is returned in the form of a query.
        :param sql_code: a string of a sql code
        :param num_of_rows: The number of top rows you want from a quarry result
        (if the argument is null then  it will just return all the rows it found)
        :param quarry_args:a dictionary/list of values you want to safely escape values into the code.
        :param filer_unique_row: if the quarry has one row, it will automatically return the row
        instead of a list of rows with one element.
        If you don't want this to happen, enter False.
        :return: a query as a list of rows. If it didn't find an rows, it will return None
        """
        with self.db_con:
            if quarry_args is None:
                quarry_args = []
            try:
                self.db_cursor.execute(sql_code, quarry_args)

                if not num_of_rows:
                    query_result = self.db_cursor.fetchall()
                elif num_of_rows > 0:
                    query_result = self.db_cursor.fetchmany(num_of_rows)
                else:
                    raise WritingError("The number of rows needs to be positive")

                if len(query_result) == 0:
                    return None
                elif len(query_result) == 1 and filer_unique_row:
                    return query_result[0]
                else:
                    return query_result
            except sqlite3.Error as e:
                print(e)
                # raise e

    def table_info(self, table_name):
        """
        Returns data on all the columns of a table.
        :param table_name: The name of the table.
        :return: a list/dictionary of all the columns' data. it will return None if there are no tables.
        It will also return columns that hold encoded python types. They will always be "longblob"

        """
        raw_table_info = self.collect_sql_quarry_result("pragma table_info({});".format(table_name))
        # This holds the information about the table as it is given by the Sqlite3 module: as a list of tuples.
        # For conveniency, I'll transform this into a dictionary, where each column_name is associated with its details.
        encrypted_columns = []
        # This will store all the names of the columns that are meant to store data types that SQL doesn't support.
        # Thier data type is "longblob" so the program can know to encrypt and decrypt them.
        if raw_table_info is None:
            return None, None

        info_dict = {}
        for info_column in raw_table_info:
            info_dict[info_column[ColumnData.name.value]] = info_column
            if info_column[ColumnData.type.value] == Database.__encoded_data_type:
                encrypted_columns.append(info_column[ColumnData.name.value])

        return info_dict, encrypted_columns

    def does_table_exists(self, table_name):
        """
        checks for the table exists in the user's database.
        I don't check if the table is in the main sql table, so hackers won't be able to gain access to the main table.
        :param table_name: the name of the table
        :return: True if it exists, False otherwise.
        """
        table_info = self.collect_sql_quarry_result("pragma table_info({});".format(table_name))
        return table_info is not None

    def create_table(self, table_sql_code, replace_table=False):
        """
        This function will check if a table with the same name already exists
        and if not, it will fill all the sizes of the column types according to the standard size for each type that
        is saved in the module.
        You just need to make sure that there is a space(and not a line break) from both sizes.
        There is a requirement that there needs to be a space(not break lines) around the column types
        and if a column is an auto increment, then it's type has to be "integer" so the program can
        distinguish it.
        :param table_sql_code: The "create" command without the limit sizes
        :param replace_table: If it's True and the table you want to create already exists, it will erase
        the current one and replace it with the
        """
        table_index = find_with_different_cases(table_sql_code, "TABLE")
        if table_index == -1:
            raise WritingError("This isn't a proper code for creating a table")
        end_index = find_with_different_cases(table_sql_code, "(")
        table_name = table_sql_code[table_index + 6:end_index - 1].strip()


        if self.does_table_exists(table_name):
            if replace_table:
                self.delete_table(table_name)
                self.execute_sql_code(table_sql_code)
        else:
            self.execute_sql_code(table_sql_code)

    @staticmethod
    def __type_load(encoded_value):
        """

        :param value_from_database: This value was loaded directly from the database
        :return:
        """
        try:
            return pickle.loads(encoded_value)
        except pickle.PickleError:
            return encoded_value

    @staticmethod
    def _transfer_list_to_dictionary(sql_code, dict_args, list_args):
        """
        This is transforming values that are ascaped via a list, to values that are escaped via a dictionary.
        :param sql_code:The original sql_code
        :param dict_args:A dictionary of arguments you want to add the list arguments.
        :param list_args:The list of arguments you want to escape.
        :return:
        """
        index = 0
        counter = 0
        s_point = 0
        finished = False

        while not finished:
            f = sql_code.find("?", s_point + 1)
            if f == -1:
                finished = True
            else:
                sql_code = sql_code[0:f] + ":a" + str(counter) + sql_code[f + 1:-1] + sql_code[-1]

                dict_args["a" + str(counter)] = list_args[counter]
                counter += 1
                if counter == len(list_args):
                    raise WritingError("The amount of question marks don't fit the number of arguments")
                f = sql_code.find("?", s_point + 1)

            if counter < len(list_args):
                raise WritingError("The amount of question marks don't fit the number of arguments")

            return dict_args, sql_code

    def load_data(self, table_name, condition=None, select_args=None, select_columns=None, distinct=False, row_num=None,
                  order_by_columns=None, order_type="ASC", filer_unique_row=True, decode_rows=True):
        """
        Loads data from a table in the database by pulling certain rows with the "SELECT" sql command.

        :param table_name:the name of the table

        :param select_columns: A list of names of the columns you want load from. If it's "None", then it will load
        all the columns.

        :param distinct: "True" if you want to load rows that their values in the wanted columns are distinct,
        "False", if you want to load them normally.

        :param condition: the condition the rows need to satisfy to be loaded. The condition needs to be written in sql.
        Make sure the user can't affect the condition, because the user can do an injection.
        If condition is None, then it will return all the rows in the table.

        :param row_num: The top number of rows you want to be loaded from what it found.
        If it is "None", it will return all the rows it found.
        :param select_args: a dictionary/list of values you want to safely escape values into the code.

        :param order_by_columns: A list of column names where, if it's not None, the order of the rows of the querry
        will be sorted by the values of those columns
        :param order_type: This will determine if the rows will be sorted in ascending or descending order,
        . If it's "asc", then it will order the row from the row with the smallest values in the columns mentioned
        in "order_by_columns" to the biggest. If it's "DESC", then it will be the opposite.
        If it's None, it will automaticly sort by asending order.

        :param filer_unique_row: if querry returns one row, it will automaticlly return the row
        instead of a list of rows with one element.
        If you don't want this to happen, enter False.

        :param decode_rows: This function will automaticly will order a row to a dictionary of columns and the values
        in them. if you want the quarry result as it is, enter false.
        :return: a list of the rows it found.
         and the data in every row is presented as a dictionary of the column name and the value
         in that column.
           It returns None if it found no columns
        in that column
        """
        info_dict, encrypted_columns = self.table_info(table_name)
        # print(table_info)
        select_code = "Select "
        if distinct:
            select_code += "distinct "

        if select_columns is None:
            select_code += "* "
        else:
            encrypted_columns = iter(set(encrypted_columns) & set(select_columns))
            #I will just iterate over this, so therre is no use of created a list that will cost more memory.
            select_code += ", ".join(select_columns)

        select_code += " \nfrom " + table_name
        if condition:
            select_code += "\n where " + condition

        if order_by_columns:
            select_code += " \n order by " + ", ".join(order_by_columns)

            if order_type.lower() in ["asc","desc"]:
                select_code += " " + order_type
            else:
                raise WritingError("This module doesn't support this ordering method")

        select_code += ";"
        raw_data = self.collect_sql_quarry_result(select_code, quarry_args=select_args, num_of_rows=row_num,
                                                  filer_unique_row=False)
        #raw data holds all quarry as it is given by the squlite3 module:
        # a list of rows, each are each a tuple of values in each column by order.
        # each row now will be translated it to a dictionary where the key of each value is the column name
        # it is saved in.

        if raw_data is None:
            return None

        if decode_rows:
            data_lines = []
            if select_columns is None:
                for raw_row in raw_data:
                    translated_row = dict(zip_longest(info_dict.keys(), raw_row))
                    for encrypted_column in encrypted_columns:
                        translated_row[encrypted_column] = self.__type_load(translated_row[encrypted_column])
                    data_lines.append(translated_row)
            else:
                for raw_row in raw_data:
                    translated_row = dict(zip_longest(select_columns,raw_row))
                    for encrypted_column in encrypted_columns:
                        translated_row[encrypted_column] = self.__type_load(translated_row[encrypted_column])
                    data_lines.append(translated_row)

            if len(data_lines) == 1 and filer_unique_row:
                return data_lines[0]
            else:
                return data_lines
        else:
            return raw_data

    def check_for_record(self, table_name, condition, distinct=False, distinct_columns=None, select_columns=None,
                         check_args=None, row_num=None, return_data=False):
        """
        Checks for records that satisfy a certain condition.
        You can't escape a table name and the condition, so you need to ensure that the user can't enter the table name
        or condition.
        :param table_name: the table name.

        :param distinct: if "return_data" is "True",
        "True" if you want to load rows that their values in the wanted columns are distinct,
        "False", if you want to load them normally.

        if "return_data" is "False" and "distinct" is "True", it will check if all the values are
        distinct. It will return

        :param distinct_columns: if "distinct" is True, then it can get a list of columns the user would like to check
        has distinct values(If distinct is False, then it will ignore it)
        :param condition: the condition the rows need to satisfy to be loaded. The condition needs to be written in sql.
        Make sure the user can't affect the condition, because the user can do an injection.

        :param select_columns: A list of names of the columns you want load from if "return_data" is "True".
        If it's "None", then it will load all the columns.
        :param row_num: The top number of rows you want to be loaded from what it found.
        If it is "None", it will return all the rows it found.

        :param check_args: a dictionary/list of values you want to safely escape values into the code of the condition.

        :param return_data: "True" for loading all the rows
        or "False" for just checking if there are records that satisfy the condition.

        :return: if "return_data" is "True", it will return a list of all the wanted rows
        (None if there are none) and if "return_data" is "False",
        it will return "True" if there are rows that satisfy the condition, "False" if there are None.
        If "distinct" is True and "Return_Data" is false, then it will also return if the condition is met and if what
        fulfilled the conditions is distinct as a tuple.
        """
        if not self.does_table_exists(table_name):
            raise WritingError("The table doesn't exist")

        if return_data:

            result = self.load_data(table_name, condition, select_args=check_args, distinct=False,
                                    select_columns=select_columns, row_num=row_num)
            return result
        else:
            sql_code = "Select * from {} where {};".format(table_name, condition)
            result = self.collect_sql_quarry_result(sql_code, quarry_args=check_args, filer_unique_row=False)
            if distinct:
                if result is None:
                    return False, False
                info_dict, encrypted_columns = self.table_info(table_name)

                if distinct_columns is None:
                    distinct_columns = info_dict.keys()

                for column_name in distinct_columns:
                    if column_name not in info_dict:
                        raise WritingError("The column doesn't exist")
                    sql_code = "Select distinct {} from {} where {};".format(column_name, table_name, condition)
                    dis_result = self.collect_sql_quarry_result(sql_code, quarry_args=check_args,
                                                                filer_unique_row=False)
                    if len(dis_result) < len(result):
                        return True, False
                return True, True
            return result is not None

    def find_specific_record(self, table_name, values, distinct=False, select_columns=None,
                             row_num=None, return_data=False):
        """
        Returns records that have a specific values in a specific columns.
        :param table_name: the table name.

        :param values: a dictionary with the name of a column pared with the value you want to check in that column.

        :param distinct: if "return_data" is "True",
        "True" if you want to load rows that their values in the wanted columns are distinct,
        "False", if you want to load them normally.

        if "return_data" is "False" and "distinct" is "True", it will check if all the values are
        distinct. It will return

        :param select_columns: A list of names of the columns you want load from if "return_data" is "True".
        If it's "None", then it will load all the columns.

        :param row_num: The top number of rows you want to be loaded from what it found.
        If it is "None", it will return all the rows it found.

        :param return_data: "True" for loading all the rows
        or "False" for just checking if there are records with these values.

        :return: if "return_rows" is "True", it will return a list of all the wanted rows
        (None if there are none) and if "return_rows" is "False",
        it will return "True" if there are rows that have these values, "False" if there are None.
        """

        condition = ""
        info_dict, encrypted_columns = self.table_info(table_name)
        if info_dict is None:
            raise WritingError("The table doesn't exist")
        first_column = True
        check_values = {}
        for column_name, value in values.items():
            if column_name not in info_dict:
                raise WritingError("You wanted to check a column that doesn't exist")

            if first_column:
                first_column = False
            else:
                condition += " and "

            condition += '{}=:column_{}'.format(column_name, column_name)
            check_values["column_" + column_name] = self.__type_dump(value,
                                                                     info_dict[column_name][ColumnData.type.value])

        return self.check_for_record(table_name, condition, distinct=distinct, distinct_columns=list(values.keys()),
                                     select_columns=select_columns, check_args=check_values, row_num=row_num,
                                     return_data=return_data)

    def delete_records(self, table_name, condition, con_args=None):
        """
        It deletes rows that satisfy the condition.
        :param table_name: the table name.
        :param con_args: a dictionary/list of values you want to safely escape values into the code of the condition.
        :param condition: the condition the rows need to satisfy to be deleted.
        The condition needs to be written in sql.
        Make sure the user can't affect the condition, because the user can do an injection.
        """

        delete_code = "delete from {} where {};".format(table_name, condition)
        self.execute_sql_code(delete_code, con_args)

    def update_records(self, table_name, values, condition=None, code_args=None):
        """
        Updates specific columns with new values in rows that satisfy the condition.
        :param table_name: the table name.

        :param values: a dictionary with the name of a column pared with the new value you update.

        :param condition: the condition the rows need to satisfy to be updated.
        If it's None, it will fill all the rows that has in a column a different value then the value you want to update
        in that column(Even though it is preferable to fill a condition).
        The condition needs to be written in sql.
        Make sure the user can't affect the condition, because the user can do an injection.

        :param code_args: A dictionary of arguemnts to ascape into the condition. If you HAVE to ascape a list, you can,
        but it will be transferred to a dictionary, which will be slow the program alot more.
        of code you want to safely escape into the code of the condition.
        If it's a dictionary, the keys need to be names that are present in where you want to escape the value to in
        the code, with ":" before it.
        """

        if not self.does_table_exists(table_name):
            raise WritingError("The table doesn't exist")

        update_code = "update " + table_name + "\n set "
        condition_code = "not ("
        info_dict, encrypted_columns = self.table_info(table_name)

        first_column = True
        value_esc = {}
        for column_name, value in values.items():
            if column_name not in info_dict:
                raise WritingError("You wanted to check a column that doesn't exist")
            if info_dict[column_name][ColumnData.pk.value] == 0:
                # The function makes sure it won't update primary keys, becuase it would be hard to find a row
                # when it'sidentifier was changed. Also it will turn a couple of primary keys to the same value.
                if first_column:
                    first_column = False
                else:
                    condition_code += " and "
                    update_code += ", "

                condition_code += '{}=:column_{}'.format(column_name, column_name)
                update_code += '{}=:column_{}'.format(column_name, column_name)
                value_esc["column_" + column_name] = Database.__type_dump(value,
                                                                          info_dict[column_name][ColumnData.type.value])

        condition_code += ")"
        if condition is None:
            update_code += "\n where " + condition_code + ";"
        else:
            update_code += "\n where " + condition + ";"

        if code_args is not None:
            if isinstance(code_args, dict):
                value_esc.update(code_args)
            elif isinstance(code_args, list):
                # This is the reason why it is prefered to ascape to an update command a dictioanry, becuase it can't
                # ascape the dictionary of values to update and the list of arguments at the same time.
                # So the program will inevitably
                value_esc, update_code = self._transfer_list_to_dictionary(update_code, value_esc, code_args)
            else:
                raise WritingError('The "code_args" is not a dictionary or a list.')

        self.execute_sql_code(update_code, code_args=value_esc)

    @staticmethod
    def __type_dump(column_value, column_type):
        """
        How this module handles data types that SQL doesn't support but python does, is by ecrpyting them via pickling,
        and saving them in a column marked by the data_type "longblob", to
        Handles a value that the program wants to write in the database by decoding it by it's special protocol.
        The protocol enables saving python types.
        :param column_value:the value you want to encrypt.
        :return: the
        """

        if type(column_value) in data_types_encode:
            return column_value
        else:
            if column_type == Database.__encoded_data_type:
                return pickle.dumps(column_value)
            else:
                raise WritingError("You are saving a data type that SQL doesn't support in a regular column.\n" +
                                   'The column needs to have the data type of "{}"'.format(Database.__encoded_data_type)
                                   + ' so that the module would know to encrypt and decrypt it.')

    def dump_data(self, table_name, insert_dict):
        """
        Inserts a row of data to a table.
        :param table_name: The name of the table you want to insert your row in.
        :param insert_dict: a dictionary filled with columns from the table and the data you want to enter in that column.
        """

        info_dict, encrypted_columns = self.table_info(table_name)
        if not info_dict:
            raise WritingError("The table doesn't exist")
            #raise WritingError("There needs to be at least one unique primary key so the row can be called") I
            #If I add the same thing twice, I won't want ti to crash, but ignore it
        # If "unique_columns_num" is zero, it means that for every value in a primry key already exists in the table
        # and isn't unique.

        first_index = True
        new_row_code = "INSERT INTO " + table_name + '('
        new_row_code_values = ' VALUES ('
        safe_escaping_dict = {}
        # To prevent level one sql injection I'm won't direcly insert the values to the code,
        # but safely escape them into the code
        for column_name, column_data in insert_dict.items():
            if column_name in info_dict:
                if first_index:
                    first_index = False
                else:
                    new_row_code += ', '
                    new_row_code_values += ', '
                new_row_code += column_name
                column_arg = "column_" + column_name
                safe_escaping_dict[column_arg] = Database.__type_dump(column_data,
                                                                      info_dict[column_name][ColumnData.type.value])

                new_row_code_values += ":" + column_arg
            else:
                raise WritingError("You want to fill a column that doesn't exist")

        new_row_code += ')'
        new_row_code_values += ')'
        new_row_code += new_row_code_values + ';'
        self.execute_sql_code(new_row_code, safe_escaping_dict)

    def create_function(self, sql_function, sql_function_name):
        self.db_con.create_function(sql_function_name, sql_function.__code__.co_argcount, sql_function)

    def close(self):
        print('Connection closed')
        self.db_cursor.close()
        self.db_con.close()

    def __del__(self, delete_database=False):
        # self.execute_sql_code("DETACH DATABASE " + self.db_name)
        self.close()
        if delete_database:
            if os.path.isfile(self.db_file_dir):
                os.remove(self.db_file_dir)

    def delete_table(self, table_name):
        if self.does_table_exists(table_name):
            self.execute_sql_code("DROP TABLE " + table_name)

    def reset_table(self, table_name):
        """
        It Erases all of a tables rows.
        :param table_name: The name of the table.
        """

        if self.does_table_exists(table_name):
            self.execute_sql_code("DELETE FROM " + table_name)
        """
        if self.does_table_exists(table_name):
            select_code = "select sql from sqlite_master where type='table' and name=:table_name;"

            table_code = self.load_data(table_name, select_code, {'table_name': table_name})[0]['sql']
            self.delete_table(table_name)
            self.create_table(table_code)
        """