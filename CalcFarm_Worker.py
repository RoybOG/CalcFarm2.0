# from bottle import route, run, template, request
import sys
import subprocess
import os
import enum
import time
import multiprocessing
import concurrent.futures
import Calc_Farm_Communications as com

ARGS = sys.argv
if len(ARGS) == 0:
    print("Something is wrong.")
    sys.exit(-1)
else:
    ARGS = ARGS[1:]
    if len(ARGS) == 0:
        print("You didn't insert your user name as as argument.")
        sys.exit(-1)

username = ARGS[0]

class WorkerWorkStatusNames(enum.Enum):
    working = 1
    finished_work = 2


class ServerWorkStatusNames(enum.Enum):
    no_work = 0
    has_work = 1
    finish_work = 2


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
        :return: returns the error message you see when this error is raised.
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
            "first num": self._first_num,
            "last num": self._lastnum,
            "error": self._error_message,
            "output": self._output
        }


PORT = 8080
MAIN_SERVER_PORT = 6060
TIMEOUT = 5
number_of_work_units = 0
sum = 0.0

Main_Server_IP = "192.168.1.101"
global time_start, time_end
#The website needs to save the IP of the computer that the server is being downloaded on
# and the website will input it to every worker

MainServerURL = "http://" + Main_Server_IP + ':' + str(MAIN_SERVER_PORT) + "/communication/worker"
class WorkerError(Exception):
    """
    When an error happened in the worker's actions or in his communiations with the server
    and the he can't work.
    When it is raise, it will send a negative message to the
    """
    pass


def send_to_work_server(id, work_server_ip, route_url, data, input_list=None):
    """
    This function sends 'post' request to one of the routes on the server
    They have a special protocol that the first argument to any url on it must be its ID so the work server can
    identify it.
    :param id: the id of the worker.
    :param work_server_ip: The ip address of the work server that the worker can connect with to it.
    :param route_url: a string of the url of the route
    :param data: the data that the worker wants to send to the server
    that is too big to be sent normally as an argument.
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :return: the output from the route(if there is), after it got the post.
    """
    if input_list is None:
        input_list = []

    input_list = [id] + input_list
    return com.post_to_route(route_url, data, work_server_ip , input_list=input_list)


def connect_to_work_server(id, work_server_ip, route_url, input_list=None):
    """
    This function sends 'get' request to one of the routes on the work server.
    They have a special protocol that the first argument to any url on it must be its ID so the work server can
    identify it.
    :param id: the id of the worker.
    :param work_server_ip: The ip address of the work server that the worker can connect with to it.
    :param route_url: a string of the url of the route
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :return: the output from the server.
    """
    if input_list is None:
        input_list = []
    input_list = [id] + input_list

    return com.connect_to_route(route_url, work_server_ip, input_list)


def create_folder(folder_name):
    folder_path = main_directory + '/' + folder_name
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)


def worker_sign_up():
    """
    In this function, the worker connects to the server for the first time and registers himself
    by getting a unique ID.
    :return: The name of the task. the work server's IP adress and
    A unique ID of a hexadecimal string that the worker will use to identify itself to the server as a tuple.
    """
    task = com.connect_to_route("get_task", MainServerURL, [username])["task"]
    while not task:
        time.sleep(5)
        task = com.connect_to_route("get_task", MainServerURL, [username])["task"]

    task_py_name = task["exe_name"]
    work_server_ip = task["server_ip"]
    work_server_url = "http://" + work_server_ip + ':' + str(PORT) + "/communication/worker"
    #This is the url of the work-server that works on this task.
    return task_py_name, work_server_url, com.connect_to_route("signup", server_ip=work_server_url)['id']


def get_file(id, work_server_ip, py_name):
    """
    In this function, the worker gets the python file from the work server.
    It will run it on the number range of the work units it gets.
    If there is a python file with the same name, it will replace it will the newer version.
    :param id: the id of the worker.
    :param work_server_ip: The ip address of the work server that the worker can connect with to it.
    :param py_name: The name of the python script of the task.
    """

    file_data = connect_to_work_server(id, work_server_ip, "getpyfile", input_list=[py_name])
    download_folder_dir = main_directory + '/' + py_folder + '/' + py_name
    if os.path.isdir(download_folder_dir):
        os.remove(download_folder_dir)
    elif not os.path.isdir(main_directory + '/' + py_folder):
        create_folder(py_folder)

    try:
        with open(download_folder_dir, 'wb') as file_creator:
            file_creator.write(file_data)
    except OSError:
        print("The program couldn't recreate the .py file")


def python_executor(id, work_server_ip,  script_name, args=None, num_of_fails=0):
    """
    This function will execute the python file.
    There are 3 data streams to the program called 'standard streams' that the program communicates with
    to it's environment: the "standard input", the "standard error" and
    the "standard output", which is where the program places its output.
    when you "print" in a program, you are putting it on the output stream, that is set to the console by default.
    The program sets the "standard output" to the "subprocess" module, and then returns it to the program.
    :param id: the id of the worker.
    :param work_server_ip: The ip address of the work server that the worker can connect with to it.
    :param script_name: (string) the name of the python file
    :param args: (list) a list of arguments the python file is supposed to get
    in the order they will be received in the file.
    :param num_of_fails:The number of times the function failed to successfully compute the work unit.
    When it happens, it will try again 3 times, and only after 3 times it tried, it would end the program
    (Of course in some cases, like if the process raised an exception, then it would crash).
    :return: the output the python file has placed in the "standard output" stream.
    """
    if args is None:
        args = []

    if not script_name.endswith('.py'):
        script_name = script_name + '.py'

    file_dir = main_directory + '/' + py_folder + '/' + script_name
    #
    # subprocess can't handle the special symbol '\' in the string of the directory, so the function replaces it with
    # the opposite slash '/', since both of them work in a directory.
    if not os.path.isfile(file_dir):
        get_file(id, work_server_ip, script_name)
    try:
        command_args = ["python", file_dir] + list(map(str, args))


        p = subprocess.Popen(command_args, stdout=subprocess.PIPE, shell=True, text=True,
                             stderr=subprocess.PIPE)

        # p - An object that is related to the process that the python file is running on.
        print("running")
        calc_start = time.time()
        output, error = p.communicate()
        calc_time = round((time.time() - calc_start), 5)
        print("Calculated work unit in {} seconds".format(calc_time))
        # If a process was run succefully, then it's exit status will be 0.
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
                return python_executor(id, work_server_ip, script_name, args=args, num_of_fails=num_of_fails)
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
            return python_executor(id, work_server_ip, script_name, args=args, num_of_fails=num_of_fails)

        # If the program couldn't run the script, it wasn't downloaded fully or is corrupted
        # (Assuming that there is no bug in the script code that crashed it).
        # The program will try and reload the file to fix the issue.


def get_work_unit(id, work_server_ip):
    """
    This function requests a work unit from the server
    :param id: the id of the worker.
    :param work_server_ip: The ip address of the work server that the worker can connect with to it.
    :return: It would return a work unit if it found one. otherwise it would just return None
    """
    work_unit = connect_to_work_server(id, work_server_ip, "getworkunit")
    return work_unit


def update(id, work_server_ip, results, status=1):
    """
    Updates the server with the results it found from the work unit he was assigned to compute.
    If the program crash, it will
    :param id: the id of the worker.
    :param work_server_ip: The ip address of the work server that the worker can connect with to it.
    :param results: The results from the calculation of the work unit.
    :param status: the number 1 if the program computed the work unit successfully and
                   the number -1 if the program failed to compute the work unit
                   The number 0 if the program crashed or ended and the server needs to forget that worker.
                   Then the server will free that work unit to be assigned to an another worker.
    :return:
    """

    worker_log = {
        'status': status,
        'results': results
    }

    return send_to_work_server(id, work_server_ip, 'update', data=worker_log)

def handle_path(path):
    path_args = (path.replace('\\', '/')).split("/")
    valid_path = path_args[0]
    for path_arg in path_args[1:]:
        if str(path_arg):
            valid_path = valid_path + '/' + str(path_arg)
    return valid_path

main_directory = handle_path(str(os.getcwd()))
py_folder = 'pyfile'


# check_for_task():
#    task recieve_data_from_server(route_url, input_list=None)


def task_calc():
    id = None
    try:
        task_py_name, work_server_ip, id = worker_sign_up()
        results = []
        work_status = WorkerWorkStatusNames.working.value
        start = time.perf_counter()
        while work_status != WorkerWorkStatusNames.finished_work.value:
            print('Started working...')
            work_unit = get_work_unit(id, work_server_ip)
            if 'fail_message' in work_unit:
                if work_unit['fail_message'] == ServerWorkStatusNames.finish_work.value:
                    work_status = WorkerWorkStatusNames.finished_work.value
                elif work_unit['fail_message'] == ServerWorkStatusNames.no_work:
                    time.sleep(7)
            else:
                print("working on work unit no' " + str(work_unit['work_unit_id']) + " between the numbers "
                      + str(work_unit['first_num']) + ':' + str(work_unit['last_num']))
                first_num = work_unit['first_num']
                last_num = work_unit['last_num']
                print('Started Calculating...')
                py_results = None
                try:
                    py_results = python_executor(id, work_server_ip, task_py_name, [first_num, last_num])
                    print('Calculated work unit successfully')
                    update(id, work_server_ip, py_results)
                except WorkUnitError as e:
                    print(e)
                    print("Fix the error in this worker. "
                          "In the meanwhile, some other worker will handle the work unit")

                    update(id, work_server_ip, None, -1)
                    time.sleep(1)
                    # the delay is to allow other workers to try and take the work unit,
                    # so the work unit won't be forever stuck with the dysfunctional worker.

            #number_of_work_units += 1

                # recieve_file_from_server('prime_range.py')
        end = time.perf_counter()
        print("Computer no' {} finished its work! in {} seconds!".format(str(id), round(end-start, 2)))
        #print("average: " + str(round(sum/number_of_work_units, 2)))
        # print(get_file('prime_range.txt'))



    except com.CommunicationError as e:
        print("As much as the client tried, it couldn't solve a connection error.")
        print("Final error:" + str(e))
    except WorkerError as e:
        print(e)
        print('The code stopped')
    except KeyboardInterrupt:
        print("Why did you kill me?")
    finally:
        if id:
            update(id, work_server_ip, None, 0)


    """
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
    """
    #Check how the worker when an excetion raissed ascapes the while loop and how the server cant tell him that he finished it's work
def worker():
    while True:
        task_calc()


if __name__ == "__main__":
    #worker_task_calc()
    with concurrent.futures.ThreadPoolExecutor() as executer:
        results = [executer.submit(task_calc) for _ in range(os.cpu_count())]


  
        #for f in concurrent.futures.as_completed(results):
        #    print(f.result())
    #Later it would go through the main server and randomly join one of the tasks that were given