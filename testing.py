import os
import getpass
import subprocess
import time
import itertools
from sys import getsizeof
import pickle
import sqlite3
import secrets
import CalcFarm_Database_Analyser_2 as db
def handle_path(path):
    path_args = (path.replace('\\', '/')).split("/")
    valid_path = path_args[0]
    for path_arg in path_args[1:]:
        if len(str(path_arg)) > 0:
            valid_path = valid_path + '/' + str(path_arg)
    return valid_path
main_directory = handle_path(str(os.getcwd()))
exe_folder = 'exe_of_tasks'

def python_executor(script_name, args=None):
    

    if args is None:
        args = []

    if not script_name.endswith('.py'):
        script_name = script_name + '.py'

    file_dir = handle_path(main_directory + '/' + exe_folder + '/' + script_name)

    # subprocess can't handle the special symbol '\' in the string of the directory, so the function replaces it with
    # the opposite slash '/', since both of them work in a directory.
    try:
        if not os.path.isfile(file_dir):
            raise FileNotFoundError
        command = "python " + file_dir
        for arg in args:
            command = command + ' ' + str(arg)
        print("running")
        t1 = time.time()
        p = subprocess.run(command, check=True, stdout=subprocess.PIPE, shell=True, text=True,
                            stderr=subprocess.STDOUT)
        #p - An object that is related to the process that the python file is running on.
        t2 = time.time()
        print('python script finished in {} seconds'.format(t2 - t1))
        # print(str(p1.stdout))
        print('work unit results: ' + str(p.stdout))
        return p.stdout
    except FileNotFoundError:
        print("python script couldn't be found")
        return None
    except subprocess.CalledProcessError as e:
        #output, error = Popen.comm
        print("The program couldn't run the file: " + str(e.output))
        #If the program couldn't run the script, it wasn't downloaded fully or is corrupted
        # (Assuming that there is no bug in the script code that crashed it).
        #The program will try and redownload the file to fix the essue.
        os.remove(file_dir)
        return None

class WorkUnitError(Exception):
    """
    This error will be raised when a worker failed to compute a work unit.
    """

    def __init__(self, arguments, error_message, output=None):
        """
        :param arguments the python file got
        (The smallest number and the biggest number in the work unit's number range) that could have caused the error.
        :param error_message:The error message it got from the Popen that explains why it failed.
        :param output: If the program still managed to compute something, then it won't be lost.
        """
        self._error_message = error_message
        self._first_num = arguments[0]
        self._last_num = arguments[1]
        self._output = output

    def __str__(self):
        """
        :return: the error message.
        """
        if self._output:
            return "failed to run work unit of {}-{} \n {} \n output: {}".format(self._first_num, self._last_num,
                                                                                 self._error_message, self._output)
        else:
            return "failed to compute work unit of {}-{}: \n {}".format(self._first_num, self._last_num,
                                                                        self._error_message)
    def get_details(self):
        """
        This returns a crash log that details to the work server why this worker couldn't calculate the work unit.
        :return:
        """
        return {
            "first num":self._first_num,
            "last num":self._lastnum,
            "error":self._error_message,
            "output":self._output
        }

def python_executor2(script_name, args=None, num_of_fails=0):
    """

    :param script_name:
    :param args:
    :param num_of_fails:The number of times the function failed to successfully compute the work unit.
    When it happens, it will try again 3 times, and only after 3 times it tried, it would end the program
    (Of course in some cases, like if the program crashed, then it
    :return:
    """
    if args is None:
        args = []

    if not script_name.endswith('.py'):
        script_name = script_name + '.py'

    file_dir = main_directory + '/' + exe_folder + '/' + script_name
    #
    # subprocess can't handle the special symbol '\' in the string of the directory, so the function replaces it with
    # the opposite slash '/', since both of them work in a directory.
    try:
        command_args = ["python", file_dir] + list(map(str,args))
        print("running")
        t1 = time.time()
        p = subprocess.Popen(command_args, stdout=subprocess.PIPE, shell=True, text=True,
                             stderr=subprocess.PIPE)
        t2 = time.time()
        # p - An object that is related to the process that the python file is running on.
        output, error = p.communicate()
        #If a process was run succefully, then it's exit status will be 0.
        if p.returncode != 0:
            if error:
                raise WorkUnitError(arguments=args,
                                    error_message=error,
                                    output=output)
            num_of_fails += 1
            print("Failed {} times. trying again...".format(num_of_fails))
            if num_of_fails == 3:
                raise WorkUnitError(arguments=args,
                                    error_message="Couldn't run the python file for an unknown reason.",
                                    output=output)
            else:
                return python_executor2(script_name, args=args, num_of_fails=num_of_fails)

        print('python script finished in {} seconds'.format(t2 - t1))
        try:

            output = eval(output)
            print('work unit results: ' + str(output))
            return output
        except (NameError, SyntaxError):
            raise WorkUnitError(arguments=args,
                                error_message="The python file didn't repr the output to an executable string",
                                output=output)
    except (subprocess.CalledProcessError, OSError) as e:
        # output, error = Popen.comm
        print("There was a system error: \n" + str(e))
        os.remove(file_dir)
        num_of_fails += 1
        print("Failed {} times. trying again...".format(num_of_fails))
        if num_of_fails == 3:
            raise WorkUnitError(arguments=args,
                                error_message=str(e),
                                output=output)
        else:
            return python_executor2(script_name, args=args, num_of_fails=num_of_fails)

        # If the program couldn't run the script, it wasn't downloaded fully or is corrupted
        # (Assuming that there is no bug in the script code that crashed it).
        # The program will try and reload the file to fix the issue.

def exe_executor(exe_name, args=None):
    """
    This function will execute the executable file.
    There are 3 data streams to a program called 'standard streams' that the program communicates with
    to it's environment: the "standard input", the "standard error" and
    the "standard output", which is where the program places its output.
    when you "print" in a program, you are putting it on the output stream, that is set to the console by default.
    The program sets the "standard output" to the "subprocess" module, and then returns it to the program.
    :param exe_name: (string) the name of the executable file
    :param args: (list) a list of arguments the executable file is supposed to get
    in the order they will be received in the file.
    :return: the output the executable file has placed in the "standard output" stream
    """

    if args is None:
        args = []

    if not exe_name.endswith('.exe'):
        exe_name = exe_name + '.exe'

    file_dir = handle_path(main_directory + '/' + exe_folder + '/' + exe_name)

    # subprocess can't handle the special symbol '\' in the string of the directory, so the function replaces it with
    # the opposite slash '/', since both of them work in a directory.
    try:
        if not os.path.isfile(file_dir):
            raise FileNotFoundError
        command = file_dir
        for arg in args:
            command = command + ' ' + str(arg)
        print("running")
        t1 = time.time()
        p1 = subprocess.run(command, check=True, stdout=subprocess.PIPE, shell=True, text=True)
        t2 = time.time()
        print('Exe file finished in {} seconds'.format(t2 - t1))
        print('work unit results: ' + str(p1.stdout))
        return p1.stdout
    except FileNotFoundError:
        print("exe file couldn't be found")
        return None
    except subprocess.CalledProcessError:
        print("The program couldn't run the file")
        #If the program couldn't run the .exe file, it wasn't downloaded fully or is corrupted
        # (Assuming that there is no bug in the exe code that crashed it).
        #The program will try and redownload the file to fix the essue.
        os.remove(file_dir)
        return None


def exe_executor2(exe_name, args=None):
    """
    This function will execute the executable file.
    There are 3 data streams to a program called 'standard streams' that the program communicates with
    to it's environment: the "standard input", the "standard error" and
    the "standard output", which is where the program places its output.
    when you "print" in a program, you are putting it on the output stream, that is set to the console by default.
    The program sets the "standard output" to the "subprocess" module, and then returns it to the program.
    :param exe_name: (string) the name of the executable file
    :param args: (list) a list of arguments the executable file is supposed to get
    in the order they will be received in the file.
    :return: the output the executable file has placed in the "standard output" stream
    """

    if args is None:
        args = []

    if not exe_name.endswith('.exe'):
        exe_name = exe_name + '.exe'

    file_dir = handle_path(main_directory + '/' + exe_folder + '/' + exe_name)

    # subprocess can't handle the special symbol '\' in the string of the directory, so the function replaces it with
    # the opposite slash '/', since both of them work in a directory.
    try:
        if not os.path.isfile(file_dir):
            raise FileNotFoundError
        command = file_dir
        for arg in args:
            command = command + ' ' + str(arg)
        print("running")
        t1 = time.time()
        p1 = os.system(command)
        t2 = time.time()
        print('Exe file finished in {} seconds'.format(t2 - t1))
        print('work unit results: ' + str(p1.stdout))
        return p1.stdout
    except FileNotFoundError:
        print("exe file couldn't be found")
        return None
    except subprocess.CalledProcessError:
        print("The program couldn't run the file")
        #If the program couldn't run the .exe file, it wasn't downloaded fully or is corrupted
        # (Assuming that there is no bug in the exe code that crashed it).
        #The program will try and redownload the file to fix the essue.
        os.remove(file_dir)
        return None


#print(os.getcwd())
exe_name = "python_range"
args = [1, 1000000]
#print(exe_name + " " + " ".join(args))
#print(python_executor2(exe_name, args))
#getpass.getpass("Enter your password: ")
#print(p)
print("hey")
#u = input("Enter str: ")
#print(u)
"""

#print(exe_executor(exe_name, args=args))
l = [1,2,3,4,5,6,7,8,9,10]
#print(l[])
f = ["a", "b", "c", "d",  "e", "f","g","p","e", "?"]
p = []
s1 = time.perf_counter()
#print("/".join(f).replace("//","/"))
#print(itertools.zip_longest())
#dic = (dict(zip(l,f)))
dic = (dict(itertools.zip_longest(l,f)))

s2 = time.perf_counter()
print(getsizeof((dic.keys())))
print(getsizeof((list(dic))))
print(dic)
print(s2-s1)
#t = ""
t = {}
e1 = time.perf_counter()
for i in range(len(l)):
    t[l[i]] = f[i]

e2 = time.perf_counter()
#print(python_executor(exe_name, args=args))
print(t)
print(e2-e1)
#import os
#print("a"+ ",".join(p)+ "b")
w=pickle.dumps(f)
print(pickle.loads(w))
print(getsizeof(f))
"""

#print(pickle.loads(secrets.token_bytes(4)))


db_con = db.Database('test')
code = """create table testing
(
	id int
		constraint testing_pk
			primary key,
	value int not null,
	name varchar not null,
	k varchar not null
);"""
#db_con.create_table(code, replace_table=True)
#db_con.dump_data("testing",{"id": 1, "value": 7654567, "name": "jhon", "k": "d"})
#print(db_con.load_data("testing", condition="id=1", select_columns=["value","name","id"]))
#print(bytes(f))
# Command to execute
# Using Windows OS command
#cmd = 'dir'
calc_t1 = time.time()
#time.sleep(3)
final_time = time.time() - calc_t1
#print(round(final_time,4))
# Using os.system() method
#print(os.system(cmd))

def type_load(encoded_value):
    """
    :param encoded_value: This value was loaded directly from the database
    :return:
    """
    try:
        return pickle.loads(encoded_value)
    except pickle.PickleError:
        return encoded_value
    #It's probably a normal byte varuble.

com = db.Database("thre1ading_db")

print(com.does_table_exists("lol"))
com.create_function(type_load, "load")
#com.execute_sql_code("update func set column_4=? where name = 'a';",[pickle.dumps([1,2,3,4])])
sqlite3.enable_callback_tracebacks(True)
#b = com.collect_sql_quarry_result("select name, load(column_4), column_3 from func where name = 'a'")
print(com.collect_sql_quarry_result("select load(?)", [pickle.dumps([1,2,3,4])]))
#print(b)

#f = pickle.dumps(b)
#print(pickle.loads(b))
