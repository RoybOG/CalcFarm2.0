# from bottle import route, run, template, request
import requests
import Calc_Farm_Communications
import json
import sys
import subprocess
import os
import enum
import time
import multiprocessing
import concurrent.futures

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
    blank = 0
    working = 1
    finished_work = 2


class ServerWorkStatusNames(enum.Enum):
    no_work = 0
    has_work = 1
    finish_work = 2



ID = None
work_status = WorkerWorkStatusNames.blank.value
PORT = 8080
MAIN_SERVER_PORT = 6060
TIMEOUT = 5
number_of_work_units = 0
sum = 0.0

Main_Server_IP = "localhost"
global time_start, time_end
#The website needs to save the IP of the computer that the server is being downloaded on
# and the website will input it to every worker

ServerURL = ""
MainServerURL = "http://" + Main_Server_IP + ':' + str(MAIN_SERVER_PORT) + "/communication/worker"
class WorkerError(Exception):
    """
    When an error happened in the worker's actions or in his communiations with the server
    and the he can't work.
    When it is raise, it will send a negative message to the
    """
    pass




def package_data(data_dict):
    """
    This function packages data to a Json file to send to the server.
    :param data_dict: dictionary that contains data to send to the client
    :return: a json file version of the data
    """
    return json.dumps(data_dict)


def send_to_server(route_url, data_dict, input_list=None):
    """
     This function sends 'post' request to one of the routes on the server
    :param route_url: a string of the url of the route
    :param data_dict: the data that the worker wants to send to the server in the form of a dictionary
    that is too big to be sent normally as an argument.
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :return: the output from the route in the form of a dictionary that was a JSON file
    """
    json_to_send = package_data(data_dict)

    if input_list is None:
        input_list = []

    if not ID is None:
        input_list = [ID] + input_list

    route_url = handle_path(route_url)

    url_to_server = ServerURL + '/' + route_url
    dir_args = [] + input_list
    for worker_input in dir_args:
        if len(str(worker_input)) > 0:
            url_to_server = url_to_server + '/' + str(worker_input)

    req = None
    try:
        req = requests.post(url=url_to_server, data={'json': json_to_send})
        req.raise_for_status()
        return req
    except requests.exceptions.HTTPError:
        raise WorkerError(Calc_Farm_Communications.HTTP_ERRORS[req.status_code])
    except requests.exceptions.ConnectTimeout:
        raise WorkerError("The server didn't respond")
    except requests.exceptions.RequestException as e:
        raise WorkerError('There was an error in communication: ' + str(e))


def connect_to_route(route_url, server_address, input_list=None, amount_of_times_crashed=0):
    """
    This function sends 'get' request to one of the routes on the server
    :param route_url: a string of the url of the route
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :param amount_of_times_crashed: The amount of time there was an error connection. Every time the worker can't
    communicate with the servers, then it will try again. If it couldn't make contact 3 times, it closes the program.
    :return: the output from the server- a JSON file the was converted to a dictionary.
    (all the clients to the server communicate to it using JSON files as a part of the protocol)
    """
    print(ServerURL)
    if input_list is None:
        input_list = []

    if ID is not None:
        input_list = [ID] + input_list

    url_to_server = server_address + '/' + route_url
    #The server port has its own slashes and they will be removed by the function and make an invalid url
    for worker_input in input_list:
        if len(str(worker_input)) > 0:
            url_to_server = url_to_server + '/' + str(worker_input)

    req = None
    try:
        req = requests.get(url=url_to_server, timeout=TIMEOUT)
        req.raise_for_status()
        return req
    except requests.exceptions.HTTPError:
        raise WorkerError(Calc_Farm_Communications.HTTP_ERRORS[req.status_code])
    except requests.exceptions.ConnectTimeout:
        raise WorkerError("The server didn't respond")
    except requests.exceptions.RequestException as e:
        raise WorkerError('There was an error in communication: ' + str(e))


def recieve_data_from_server(route_url, input_list=None, server_address=None):
    """
    The worker gets data from a route on the server as a JSON file according to it protocol

    :param route_url: the url where the wanted route is
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :param server_address: The Main server or the Work Server.
    Firstly It will first connect to the main server to get the ip of the work server it got attached to.
    Afterwards, it will connect to that work server to get work units.
    :return: the output from the route in the form of a dictionary that was a JSON file

    """
    if server_address is None:
        server_address = ServerURL

    raw_data = connect_to_route(route_url, server_address, input_list)
    if raw_data is None:
        return None
    else:
        return raw_data.json()


def create_folder(folder_name):
    folder_path = handle_path(main_directory + '/' + folder_name)
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)


def worker_sign_up():
    """
    In this function, the worker connects to the server for the first time and registers himself
    by getting a unique ID.
    :return: A unique ID of a hexadecimal string that the worker will use to identify itself to the server.
    """
    global ServerURL
    global task_exe_name
    global conditional
    task = recieve_data_from_server("get_task", [username], MainServerURL)["task"]
    while task is None:
        time.sleep(5)
        task = recieve_data_from_server("get_task", [username], MainServerURL)["task"]

    task_exe_name, conditional = task["exe_name"], task["Task_conditional"]
    server_ip = task["server_ip"]
    ServerURL = "http://" + server_ip + ':' + str(PORT) + "/communication/worker"
    return recieve_data_from_server("signup")['id']


def exe_executor(exe_name, args=None):
    """
    This function will execute the executable file.
    There are 3 data streams to a program called 'standard streams' that the program communicates with
    to it's environment: the "standard input", the "standard error" and
    the "standard output", which is where the program places its output.
    when you "print" in a program, you are putting it on the output stream, that is set to the console by default.
    The program sets the "standard output" to the "subprocess" module, and then returns it to the program.
    :pa*ram exe_name: (string) the name of the executable file
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
            get_file(exe_name)
        if not os.path.isfile(file_dir):
            raise FileNotFoundError
        command = file_dir
        for arg in args:
            command = command + ' ' + str(arg)
        print(file_dir)
        p1 = subprocess.run(command, check=True, stdout=subprocess.PIPE, shell=True, text=True)

        # print(str(p1.stdout))
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
        get_file(exe_name)
        return None


def get_file(exe_name):
    """
    In this function, the worker gets he exe.file
    :return: A unique ID of a hexadecimal string that the worker will use to identify itself to the server.
    """

    file_data = connect_to_route("getexefile", ServerURL, input_list=[exe_name])
    download_folder_dir = handle_path(main_directory + '/' + exe_folder + '/' + exe_name)
    if not os.path.isdir(main_directory + '/' + exe_folder):
        create_folder(exe_folder)
    try:
        with open(download_folder_dir, 'wb') as file_creator:
            file_creator.write(file_data.content)
    except OSError:
        print("The program couldn't recreate the .exe file")


def get_work_unit():
    """
    This function requests a work unit from the server
    :return: It would return a work unit if it found one. otherwise it would just return None
    """
    work_unit = recieve_data_from_server("getworkunit")
    if 'fail_message' in work_unit:

        if work_unit['fail_message'] == ServerWorkStatusNames.finish_work.value:
            global work_status
            work_status = WorkerWorkStatusNames.finished_work.value
        return None
    else:
        print("working on work unit no' " + str(work_unit['work_unit_id']) + " between the numbers "
              + str(work_unit['first_num']) + ':' + str(work_unit['last_num']))
        return work_unit


def update(results, status=1):
    """
    Updates the server with the results it found from the work unit he was assigned to compute.
    If the program crash, it will
    :param results:
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

    return send_to_server('update', worker_log)

def handle_path(path):
    path_args = (path.replace('\\', '/')).split("/")
    valid_path = path_args[0]
    for path_arg in path_args[1:]:
        if len(str(path_arg)) > 0:
            valid_path = valid_path + '/' + str(path_arg)
    return valid_path

main_directory = handle_path(str(os.getcwd()))
exe_folder = 'exefile'


# check_for_task():
#    task recieve_data_from_server(route_url, input_list=None)


def worker_task_calc():
    global ID
    global task_exe_name
    try:
        ID = worker_sign_up()
        results = []
        global work_status
        global number_of_work_units
        global sum
        work_status = WorkerWorkStatusNames.working.value
        start = time.perf_counter()
        while work_status != WorkerWorkStatusNames.finished_work.value:

            print('Started working...')

            work_unit = get_work_unit()

            print('Got work unit...')
            if work_unit is None:
                time.sleep(5)
            else:
                first_num = work_unit['first_num']
                last_num = work_unit['last_num']
                print('Started Calculating...')
                calc_t1 = time.perf_counter()
                exe_results = exe_executor(task_exe_name, [first_num, last_num])
                calc_t2 = time.perf_counter()
                print("Calculated work unit in {} seconds".format(round(calc_t2 - calc_t1, 2)))
                print('Finished Calculating...')
                if exe_results is None:
                    print("Fix the error in this worker. "
                          "In the meanwhile, some other worker will handle the work unit")

                    update(None, -1)
                    time.sleep(1)
                    # the delay is to allow other workers to try and take the work unit,
                    # so the work unit won't be forever stuck with the dysfunctional worker.

                else:
                    print('Calculated work unit successfully')
                    try:

                        update(eval(exe_results))

                    except (NameError, SyntaxError):
                        raise WorkerError("The exe didn't repr the output to an executable string")

            number_of_work_units += 1
            sum =+calc_t2-calc_t1

                # recieve_file_from_server('prime_range.exe')
        end = time.perf_counter()
        print("Computer no' {} finished its work! in {} seconds!".format(str(ID), round(end-start, 2)))
        print("average: " + str(round(sum/number_of_work_units, 2)))
        # print(get_file('prime_range.txt'))



    except ConnectionError:
        print('Connetion with the server has lost')
    except WorkerError as e:
        print(e)
        print('The code stopped')
    except KeyboardInterrupt:
        print("Why did you kill me?")
    finally:
        if ID is not None:
            update(None, 0)


    """
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
    """
    #Check how the worker when an excetion raissed ascapes the while loop and how the server cant tell him that he finished it's work
if __name__ == "__main__":
    """
    with concurrent.futures.ThreadPoolExecutor() as executer:
        results = [executer.submit(worker_task_calc) for _ in range(2)]
    """
    worker_task_calc()

        #for f in concurrent.futures.as_completed(results):
        #    print(f.result())
    #Later it would go through the main server and randomly join one of the tasks that were given