from bottle import route, run, post, request, abort, static_file, redirect
import uuid
import json
import socket
import os
import sys
import time
import threading
import Calcfarm_server_database_connector as Db
from Calc_Farm_Essential import *
TIMEOUT = 5
# If the servers don't respond after TIMEOUT seconds, the worker disconncts and tries again.
PORT = 8080
# The port of the worker_server
MAIN_SERVER_PORT = 6060
# The port of the main server.
# The main server should have a different port from the rest of the work server to distinguish
# itself.
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

Main_Server_IPAddr = "10.0.0.11"
#Interesting how computers have differnt IPs in different wifis
Main_Server_url = "http://" + Main_Server_IPAddr + ':' + str(MAIN_SERVER_PORT) + '/communication/work_server'

global db_lock
global py_name
global Task_Conditional
work_units_amount = 0
finished_work_units_amount = 0
WORK_UNIT_LENGTH = 100000
work_status = Db.WorkStatusNames.no_work.value
global time_start, time_end
started_working = False

num_of_computers = 0
HTTPSTATUSCODES = {"Bad Request": 400,
                   "Unauthorized": 401,
                   "Forbidden": 403,
                   "Not Found": 404,
                   "Not Allowed": 405,
                   "I'm a teapot": 418,
                   "Internal Server Error": 500
                   }

HTTPSTATUSCODESDECODER = {400: "Bad Request",
                          401: "Unauthorized",
                          403: "Forbidden",
                          404: "Not Found",
                          405: "Not Allowed",
                          418: "I'm a teapot",
                          500: "Internal Server Error"
                          }
HTTPSTATUSMESSAGES = {"Bad Request": "The server did not recognise the command",
                      "Unauthorized": "The server doesn't recognise you.",
                      "Forbidden": "You aren't qualified to get access to this service",
                      "Not Found": "The resource you asked for doesn't exist.",
                      "Not Allowed": "Your http request was invalid",
                      "I'm a teapot": "I'm sorry kind sir, I can't offer you tea!",
                      "Internal Server Error": "There was an error in a process inside the server"
                      }


def handle_path(path):
    path_args = (path.replace('\\', '/')).split("/")
    valid_path = path_args[0]
    for path_arg in path_args[1:]:
        if len(str(path_arg)) > 0:
            valid_path = valid_path + '/' + str(path_arg)
    return valid_path


def create_folder(folder_dir):
    folder_path = handle_path(folder_dir)
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)


py_folder = 'tasks'
main_directory = os.getcwd().replace('\\', '/')
py_dir = main_directory + '/' + py_folder
# The folder with all of the website files is saved in the current working directory(cwd),
# the directory where the ".py" file of this program is.
create_folder(py_dir)


# Send Functions:

def connect_to_main_server(route_url=None, input_list=None):

    """
    This function sends 'get' request to one of the routes on the main server.
    :param route_url: a string url of the route that is inside the "work_server" route in the Main Server.
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :return: the output from the server.
    """

    if input_list is None:
        input_list = []

    input_list = [username] + input_list

    if route_url is None:
        route_url = ''

    return connect_to_route(route_url, Main_Server_url, input_list)


def send_to_server(route_url, data, input_list=None):
    """
     This function sends 'post' request to one of the routes on the Main Server
    :param route_url: a string url of the route that is inside the "work_server" route in the Main Server.
    :param data_dict: the data this work_server wants to send to the server
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :return: the output from the route(if there is), after it got the post.
     route.
    """

    if input_list is None:
        input_list = []

    input_list = [username] + input_list

    return post_to_route(route_url, data, Main_Server_url, input_list=input_list)


def get_file(requested_py_name):
    """
    In this function, the work server gets from the main server the python file of the task it's working on.
    :param requested_py_name: the name of the requested .py file.
    :return: A unique ID of a hexadecimal string that the worker will use to identify itself to the server.
    """

    file_data = connect_to_main_server("getpyfile", input_list=[requested_py_name])
    if not requested_py_name.endswith('.py'):
        requested_py_name = requested_py_name + '.py'
    download_folder_dir = handle_path(main_directory + '/' + py_folder + '/' + requested_py_name)
    if not os.path.isdir(main_directory + '/' + py_folder):
        create_folder(py_folder)
    try:
        with open(download_folder_dir, 'wb') as file_creator:
            file_creator.write(file_data)
    except OSError:
        print("The program couldn't recreate the .py file")


# Output funcitons


def task_divider(first_num, last_num):
    """
    This function divides the total amount of numbers to small work unit of constant amount.
    :param first_num: the smallest number in the entered number range(including that number).
    :param last_num: the bigest number in the entered number range(including that number).
    """

    num_amount = last_num - first_num + 1
    index = first_num
    reminder = num_amount % WORK_UNIT_LENGTH
    global db_lock
    global work_units
    global work_units_amount
    global work_status
    print("loading")
    work_unit_time1 = time.time()
    #with db_lock:
    while index - 1 + WORK_UNIT_LENGTH <= last_num - reminder:
        work_unit_data = {
            "first_num": index,
            "last_num": index - 1 + WORK_UNIT_LENGTH,
        }
        Db.insert_work_unit(work_unit_data)
        work_units_amount = work_units_amount + 1
        index = index + WORK_UNIT_LENGTH

    if reminder > 0:
        work_unit_data = {
            "first_num": last_num - reminder + 1,
            "last_num": last_num
        }
        Db.insert_work_unit(work_unit_data)
        work_units_amount = work_units_amount + 1
    work_unit_total_time = time.time() - work_unit_time1
    print("finished in {} seconds".format(work_unit_total_time))
    work_status = Db.WorkStatusNames.has_work.value
        # print(work_units)


def raise_http_error(status_name, status_details=None):
    """
    This function sends an HTTP error message to the client after he did an act the server can't or won't handle
    and ends the communication of him/her with the server.
    :param status_name: the offical status name(that is in the dictionary of status codes that the server was configured
    to handle: "HTTPSTATUSCODES") of the existing status code.
    :param status_details: additional details that specify what went wrong.
    """

    status_message = HTTPSTATUSMESSAGES[status_name]
    if status_details is not None:
        status_message += ':' + status_details

    if status_name in HTTPSTATUSCODES:
        abort(HTTPSTATUSCODES[status_name], status_message)


def read_file(root, file_name, file_type='t'):
    """
    This function finds a file in the 'root' folder with the corresponding name,
    and reads its content depending on it's type.

    :param root: the folder in which the file is: if it is 'website', then it's a html file to the website
    and if it is the "py_folder" then it's the python file.

    :param file_name: the name of the text file(including the file type at the end, which in this case is '.txt')
    that has the html of the wanted page.

    :param file_type: the type of file's content.  't' for text files, 'b' for binary files.
    By default, it would assume the file is a text file.
    :return: the html code that was stored on the file as a string
    """

    page_dir = handle_path(main_directory + '/' + root + '/' + file_name)

    try:
        with open(page_dir, 'r' + file_type) as page_reader:
            return str(page_reader.read())
    except FileNotFoundError:
        print("The file the user requested doesn't exist")
        raise_http_error("Not Found")
    except OSError:
        print("The server couldn't get the page file")
        raise_http_error("Internal Server Error")


def id_generator():
    """
    This function generates an ID for A computer that sign up and wants to join and conribute.
    It uses the uuid function to generate huge unique and random strings.
    :return: a unique and long hexadecimal string that
    """
    new_id = uuid.uuid4()
    return new_id.hex


def identify(sus_id):
    """
    This function checks if a worker with this id exists in the database.
    if it doesn't exists, it will send an "Unauthorized" status error,
    it it does exists, it will return data it has about the worker.
    The worker will always send its id as the first argument to every route.

    :param sus_id: the suspicious id the program wants to check

    :return: returns data about the worker if it exists.
    """
    worker_data = Db.find_worker(sus_id)
    if worker_data is None:
        raise_http_error("Unauthorized")

    return worker_data


def package_data(data_dict):
    """
    This function packages data to a Json file to send to the client.
    :param data_dict: dictionary that contains data to send to the client
    :return: a json file of the data
    """
    return json.dumps(data_dict)


def startworking():
    """
    The first thing a work server does is ask the main server for tasks to work on.
    If there is, it will get all the details of that task:
    its name, its python file, its first number and last number in its total range.
    It will
    """
    # In the future have the manager program run this all the time.
    global py_name
    global Task_Conditional
    global db_lock
    task_data = connect_to_main_server("get_task")
    task_name = task_data.get("Task_name", None)
    #A part of thier protocol is if the main server doesn't have tasks to give, then it will send to any running
    #work server {"exe_name":None} to say there are no tasks that are untouched.
    while not task_name:
        time.sleep(5)
        task_data = connect_to_main_server("get_task")
        task_name = task_data.get("Task_name", None)

    print('Working on the task "{}"'.format(task_name))
    get_file(task_data["exe_name"])
    Task_Conditional = task_data["Task_conditional"]
    # db_lock = threading.lock()
    task_divider(task_data["first_num"], task_data["last_num"])


@route('/')
def lol():
    print('lol')
    return 'lol'


@route('/communication/worker/signup')
def signup():
    """
    The worker who communicated to this server for the first time, will get a unique ID.
    :return:
    """
    if work_status == Db.WorkStatusNames.finished_work.value:
        raise_http_error("Forbidden", "This server doesn't need anymore workers, it finished it's job")
    new_worker_id = str(id_generator())
    print("New worker entered " + new_worker_id)
    sign_up_data = {"worker_id": new_worker_id,
                    "worker_ip": request.environ.get('HTTP_X_FORWARDED_FOR') or request.environ.get('REMOTE_ADDR')
                    }
    Db.insert_worker(sign_up_data)
    return package_data({"id": new_worker_id})


@route('/communication/worker/getworkunit/<worker_id>')
def getworkunit(worker_id):
    """
    The server gets the worker an available work unit to work on.

    :param worker_id:
    :return:if it finds a work unit, it would send a JSON file of the work unit and it's information,
            but if it didn't find, it would send a message detailing to it why it couldn't find.
    """

    worker_data = identify(worker_id)
    global time_start
    global started_working
    global work_status
    if work_status == Db.WorkStatusNames.has_work.value:

        saved_work_unit = Db.get_free_work_unit()
        if saved_work_unit is None:
            work_status = Db.WorkStatusNames.no_work.value
        else:
            if not started_working:
                print("Starting to work!")
                time_start = time.time()
                started_working = True
                # It counts it's
            print(str(saved_work_unit["work_unit_id"]) + " " + str(saved_work_unit))
            Db.assign_work_unit(saved_work_unit["work_unit_id"], worker_id)
            return saved_work_unit

    return package_data({"fail_message": work_status})


@route('/lol')
def a():
    return "lol"


@post('/communication/worker/update/<worker_id>')
def update(worker_id):
    worker_data = identify(worker_id)
    global finished_work_units_amount
    global work_status
    global time_end
    worker_log_dict = Recieve_information_from_client()
    work_unit = Db.get_work_unit_by_worker(worker_id)

    if worker_log_dict['status'] == 1:
        finished_work_units_amount += 1
        Db.update_results(work_unit['work_unit_id'])
        result_log = {}
        work_unit["results"] = worker_log_dict["results"]
        work_unit.pop("work_unit_status")
        result_log["work_unit"] = work_unit
        result_log["progress_percentage"] = 100.0 * finished_work_units_amount / work_units_amount
        send_to_server("get_work_unit_results", result_log)
        if finished_work_units_amount == work_units_amount:
            time_end = time.time()
            work_status = Db.WorkStatusNames.finished_work.value
            results = Db.collect_results()
            send_to_server('get_results', {"results": results})
            print(results)
            print("Made in {} seconds".format(time_end - time_start))
    else:
        Db.free_work_unit_from_worker(worker_id)
        if worker_log_dict['status'] == 0:
            Db.remove_worker(worker_id)


@route('/communication/main_server/stop_working')
def stop_working():
    sys.exit()


@route('/communication/worker/getpyfile/<worker_id>/<requested_py_name>')
def getpyfile(worker_id, requested_py_name):
    worker_data = identify(worker_id)
    file_dir = handle_path(main_directory + '/' + py_folder)
    if not requested_py_name.endswith('.py'):
        requested_py_name = requested_py_name + '.py'

    # data_to_worker = {'file': static_file(filename, root=file_dir, download=filename)}
    # return package_data(data_to_worker)
    if not os.path.isfile(handle_path(file_dir + '/' + requested_py_name)):
        print("The server didn't find the file.")
        raise_http_error("Not Found")
    return static_file(requested_py_name, root=file_dir, download=requested_py_name)

hostname = socket.gethostname()
IPAddr = socket.gethostbyname(hostname)
print("Server {} of IP {} is ready to go!".format(hostname, IPAddr))

print("http://" + IPAddr + ':' + str(PORT))
# print(connect_to_main_server('lol').content)
results = [1, 2, 3, 4, 5, 6, 7, 8, 9]
# send_to_server('get_results',{"results":results})
startworking()
run(host='0.0.0.0', port=PORT)
# run(host='localhost', port=PORT)

"""
@route('/communication/worker/signup/<comupter_id>')
def signup(comupter_id):

    The worker who communicated to this server for the first time, will get
    :return:

    identify(comupter_id , computers)
    new_worker_id = str(id_generator())
    global workers
    workers[new_worker_id] = {}
    workers[new_worker_id]['computer_id'] = comupter_id
    workers[new_worker_id]['status'] = Db.WorkerStatusNames.just_joined.value
    sign_up_data = {"id": new_worker_id}
    return package_data(sign_up_data)
)
"""