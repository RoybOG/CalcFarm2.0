import CalcFarm_Database_Analyser_2 as Db
from Calc_Farm_Essential import *
import os
import hashlib
import math
from random import choice

class MainServerError(Exception):
    """

    """
    pass


task_status_names_list = ["untouched", "in_progress", "finished"]

HTTPSTATUSCODES = {"Bad Request": 400,
                   "Unauthorized": 401,
                   "Forbidden": 403,
                   "Not Found": 404,
                   "I'm a teapot": 418,
                   "Internal Server Error": 500
                   }
HTTPSTATUSCODESDECODER = {400: "Bad Request",
                   401: "Unauthorized",
                   403: "Forbidden",
                   404: "Not Found",
                   418: "I'm a teapot",
                   500: "Internal Server Error"
                   }
HTTPSTATUSMESSAGES = {"Bad Request": "The server did not recognise the command",
                      "Unauthorized": "The server doesn't recognise you.",
                      "Forbidden": "You aren't qualified to get access to this service",
                      "Not Found": "The resource you asked for doesn't exist.",
                      "I'm a teapot": "I'm sorry kind sir, I can't offer you tea!",
                      "Internal Server Error": "There was an error in a process inside the server"
                      }


task_form_details = [
    {'form_name': 'task_name', 'form_text': 'Task Name:', 'type': 'text', 'input_type': str,
     'form_text_after': '<p>wronglol</p>:'},
    {'form_name': 'first_num', 'form_text': '<br>number range:', 'type': 'text', 'input_type': int},
    {'form_name': 'last_num', 'form_text': ' - ', 'type': 'text', 'input_type': int}
]

signup_form_details = [
    {
    'form_name': 'user_name', 'form_text': '<p>Enter username:</p>',
     'form_text_after': '<br><p class = "warnings" id="user_name_warning"></p>',
     'type': 'text', 'input_type': str},
    {'form_name': 'password', 'form_text': '<br>Enter password', 'type': 'password', 'input_type': str}
    ]

login_form_details = [
    {'form_name': 'user_name', 'form_text': '<p>Enter username:</p>',
     'form_text_after': '<br><p class = "warnings" id="password_warning"></p>',
     'type': 'text', 'input_type': str},
    {'form_name': 'password', 'form_text': '<br>Enter password', 'type': 'password', 'input_type': str}
    ]


def handle_path(path):
    valid_path = '/'.join(list(filter(lambda x: len(x) > 0, path.replace('\\', '/').split("/"))))
    # print(valid_path)
    # print(os.path.isfile(valid_path))
    return valid_path


def create_folder(folder_name):
    folder_path = handle_path(main_directory + '/' + folder_name)
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)

main_directory = handle_path(os.getcwd())
results_folder = 'results'
website_folder = 'website'
website_dir = main_directory + '/' + website_folder
pages_folder = 'pages'
images_folder = 'images'
script_folder = 'code_pages'
py_folder = 'py_of_tasks'

create_folder(results_folder)
create_folder(website_folder)
create_folder(website_folder + '/' + pages_folder)
create_folder(website_folder + '/' + images_folder)
create_folder(website_folder + '/' + script_folder)
create_folder('/' + py_folder)
base_dir = website_dir + '/' + pages_folder + '/base.html'
base_form_dir = website_dir + '/' + pages_folder + '/base_form.html'

database_handler = Db.Database('Calc_Farm_Main_Server_database')

tables_code = []

tables_code.append("""
create table online_data
(
	user_name varchar not null,
	password varchar not null,
	constraint table_name_pk
		unique (user_name, password)
);
""")

tables_code.append("""
create table sessions
(
	sid varchar not null
		constraint sessions_pk
			primary key,
	username varchar not null
		constraint sessions_online_data_user_name_fk
			references online_data (user_name)
);
""")

tables_code.append("""
create table Tasks
(
	Task_name varchar,
	user_name varchar
		constraint table_name_online_data_user_name_fk
			references online_data (user_name),
	first_num int not null,
	last_num int not null,
	exe_name varchar not null,
	work_force_percentage int,
	Task_conditional varchar,
	    constraint table_name_pk
		    primary key (user_name, Task_name)
);
""")

tables_code.append("""
create table current_tasks
(
	Task_name varchar
		constraint current_tasks_Tasks_Task_name_fk
			references Tasks (Task_name),
	user_name varchar
	    constraint table_name_online_data_user_name_fk
			    references online_data (user_name),
	Task_status int default {},
	progress_percentage int default 0,
	last_consecutive_work_unit int default 0,
	server_status int default {},
	work_force_percentage int,
	Task_Connected_workers int default 0,
	server_ip varchar,
	Task_results_table varchar,
	Task_results_file varchar,
	work_units_file varchar
);
""".format(TaskStatusNames.untouched.value, WorkServerStatusNames.no_work.value))
#Plase do that every time the server's status changes, it will update the main server in "Server_Status
for table_code in tables_code:
    database_handler.create_table(table_code)
    #database_handler.create_table(table_code, replace_table=True)
#For debug perposes, it will replace the tables, change it later to "False"!

def __encode_importent_info(info):
    """
    Encodes importent information such that hackers won't understand.
    :param info: The information you want to encode as a string.
    :param return_string: Weather you want the output to be a normal string(False), or encoded(True)
    :return: the information you inputted encoded as a string of hexadecimals.
    """
    return hashlib.sha256(str(info).encode()).hexdigest()


#There is a problem with this solution, if a hacker gets the original string username and the encryption
# mathod, he can encode it in that mathod and look for the encoded version in the database instead.
#and if he finds the encoded version, then
#When the user is going to log in, he will insert the user_name and password, which will be sent to the server


def create_session(sid, username, prev_sid=None):
    """
    Writes a session in the "sessions" data table.
    :param sid: a random generated id the server created for the session.
    :param username: The name of the user that is log in this session.
    :param prev_sid: If the use is loged in another computer, then it will delleet
    :return:
    """
    if prev_sid:
        database_handler.delete_records("sessions", "sid=?", [prev_sid])

    if database_handler.find_specific_record("sessions", {"username": username}):
        database_handler.delete_records("sessions", "username=?", [username])

    database_handler.dump_data('sessions', {"sid": sid, "username": username})


def delete_session(sid):
    database_handler.delete_records("sessions", "sid=?", [sid])


def find_user(username, return_data=False):
    return database_handler.find_specific_record("online_data", {"user_name": username},
                                                 return_data=return_data)

def find_user_name_by_password_checking(username, password):
    encoded_suspisoius_password = __encode_importent_info(password)
    user_row = database_handler.find_specific_record("online_data", {"user_name": username,
                                                                     "password": encoded_suspisoius_password},
                                                     return_data=True)
    return user_row

def find_user_name_by_sid(sid):
    user_row = database_handler.find_specific_record("sessions", {"sid": sid}, select_columns=["username"],
                                                 return_data=True)
    if user_row is None:
        return None
    else:
        return user_row["username"]

def insert_user(user_name, password):
    if find_user(user_name):
        raise MainServerError("This user already exists, try using a different one")
    else:
        user_data = {'user_name': user_name, 'password': __encode_importent_info(str(password))}
   # if not __user_exists(encoded_username):
        database_handler.dump_data("online_data", user_data)


def insert_task(user_name, task_data):
    if find_user(user_name):
        task_name = task_data["Task_name"]
        task_data["user_name"] = user_name
        if not database_handler.find_specific_record("Tasks", {"Task_name": task_name, "user_name": user_name}):
            database_handler.dump_data("Tasks", task_data)
    else:
        raise MainServerError("The username doesn't exist.")


def start_working_on_a_task(user_name, task_name):
    task_data = database_handler.find_specific_record("Tasks",
                                                      values={"user_name": user_name, "Task_name": task_name},
                                                      return_data=True)
    if not task_data:
        raise MainServerError("The task of that user doesn't exist")

    if user_name != task_data["user_name"]:
        raise MainServerError("The username doesn't match the task.")

    if not database_handler.find_specific_record("current_tasks", {"Task_name": task_name, "user_name": user_name}):
        normal_tasks = database_handler.check_for_record("current_tasks",
                                                            condition="user_name=? and work_force_percentage is null",
                                                                   check_args=[user_name])


        total_percent = database_handler.collect_sql_quarry_result("select sum(work_force_percentage)"
                                                                   " from current_tasks where user_name=?"
                                                                   " and work_force_percentage is not null",
                                                                   [user_name])[0]

        if task_data.get("work_force_percentage", None):
            if total_percent:
                total_percent += task_data["work_force_percentage"]
            else:
                total_percent = task_data["work_force_percentage"]
        else:
            normal_tasks = [task_data]

        if total_percent:
            if total_percent > 100:
                raise MainServerError("The percentages add to more then the whole")
            elif total_percent == 100 and normal_tasks:
                raise MainServerError("The percentages are too high and don't leave space to normal tasks.")

        columns = ("work_unit_id", "first_num", "last_num", "worker_id", "failed_count", "results")
        database_handler.create_table(
        """
        CREATE TABLE {}_{}_Results_Table
        (
        	{} int
        		constraint work_unit_results_pk
        			primary key,
        	{} int not null,
            {} int not null,
            {} varchar not null,
            {} int,
        	{} longblob not null
        )
        """.format(*((task_name, user_name,) + columns)), replace_table=True)
        database_handler.dump_data("current_tasks",
                                   {"Task_name": task_data["Task_name"], "user_name": user_name,
                                    "work_force_percentage": task_data["work_force_percentage"],
                                    "Task_results_file": "{}_{}_Results_file".format(task_name, user_name),
                                    "work_units_file": "{}_{}_Work_Units_file".format(task_name, user_name),
                                    "Task_results_table": "{}_{}_Results_Table".format(task_name, user_name)})
        # append_to_file("{}_{}_Work_Units_file".format(task_name, user_name), "Work_Unit_File\n")
        # append_to_file("{}_{}_Work_Units_file".format(task_name, user_name), columns)



def cancel_working_on_task(user_name, task_name):
    task_data = get_task_process_details(user_name, task_name)
    if task_data:
        database_handler.delete_records("current_tasks", "Task_name=:task_name and user_name=:user_name",
                                        con_args={"task_name": task_name, "user_name": user_name})
        database_handler.delete_table(task_data["Task_results_table"])


def find_task(task_name, user_name, return_task_data=False):
    return database_handler.find_specific_record("Tasks",
                                                      values={"Task_name": task_name,
                                                              "user_name": user_name},
                                                      return_data=return_task_data, row_num=1)


def get_all_tasks_by_user(user_name):
    tasks_row = database_handler.load_data("Tasks", condition="user_name=?", select_columns=["Task_name"], select_args=
    [user_name], filer_unique_row=False)
    if not tasks_row:
        return None

    return [row["Task_name"] for row in tasks_row]


def get_all_running_tasks_by_user(user_name):
    tasks_row = database_handler.load_data("current_tasks", condition="user_name=?",
                                           select_args=[user_name], filer_unique_row=False)
    if not tasks_row:
        return None

    return tasks_row


def get_free_tasks(user_name):
    current_task_data = database_handler.find_specific_record("current_tasks",
                                                      values={"user_name": user_name,
                                                              "Task_status": TaskStatusNames.untouched.value},
                                                      return_data=True, row_num=1)

    # Here I use load data becuase an int doesn't need special treamtment to be encoded
    if current_task_data is None:
        return None
    else:
        task_data = find_task(current_task_data["Task_name"], user_name, return_task_data=True)
        if task_data is None:
            raise MainServerError('In "Current Tasks" there is listed a task that does not exist')
        else:
            return task_data


def get_task_process_details(user_name, task_name):
    return database_handler.find_specific_record("current_tasks",
                                                      values={"user_name": user_name, "Task_name": task_name},
                                                      return_data=True)

"""
def get_server_ip(task_name, username):
    task_ip = get_task_process_details()
    if task_ip is None:
        #raise MainServerError('The task that does not exist.')
        return None
    else:
        return task_ip["server_ip"]
"""

def assign_task(user_name, task_name, work_server_ip):
    """
    Assigns a task to an available work server.
    :param task_name: The name of a task that is waiting to be calculated by a work server.
    :param user_name: The name of the user that created the task and ran the work server.
    :param work_server_ip: The IP of the avaliable work server that will work on this task.
    With this IP, other workers that are assigned to this task will connect to this work server.
    """

    database_handler.update_records("current_tasks",
                                   {"server_ip": work_server_ip, "Task_status": TaskStatusNames.in_progress.value},
                                   condition="Task_name=:Task_name and user_name=:user_name",
                                    code_args={"Task_name": task_name, "user_name": user_name})


def add_work_unit(user_name, work_server_ip, result_log):
    """
    Adds a work unit the work server finished calculating to its database.
    :param user_name: The user of the work server
    :param work_server_ip: The IP of the work server
    :param result_log: a log from the server about the process of calculating the task.
    Includes the work unit, its results, the percentage of progress it managed to do.
    """
    task_details = database_handler.find_specific_record("current_tasks",
                                                          {"server_ip": work_server_ip, "user_name": user_name},
                                                          return_data=True)
    if task_details and result_log:
        results_table = task_details["Task_results_table"]
        work_unit = result_log["work_unit"]
        database_handler.dump_data(results_table, work_unit)
        if work_unit["work_unit_id"] == task_details["last_consecutive_work_unit"] + 1:

            if is_task_conditional(user_name, task_details["Task_name"]):
                pass
            else:
                new_last_id = work_unit["work_unit_id"]
                add_results_to_files_default(user_name, task_details["Task_name"], results_table, work_unit)
                new_work_unit = database_handler.find_specific_record(results_table, {"work_unit_id": new_last_id + 1},
                                                                      return_data=True)
                while new_work_unit:
                    add_results_to_files_default(user_name, task_details["Task_name"], results_table, new_work_unit)
                    new_last_id += 1
                    new_work_unit = database_handler.find_specific_record(results_table,
                                                                          {"work_unit_id": new_last_id + 1},
                                                                          return_data=True)
                database_handler.update_records("current_tasks",
                                                values={"progress_percentage": result_log["progress_percentage"],
                                                        "last_consecutive_work_unit": new_last_id},
                                                condition="server_ip=:server_ip and user_name=:user_name",
                                                code_args={"server_ip": work_server_ip, "user_name": user_name})
        else:
            database_handler.update_records("current_tasks",
                                        values={"progress_percentage": result_log["progress_percentage"]}
                                        ,condition= "server_ip=:server_ip and user_name=:user_name",
                                        code_args={"server_ip": work_server_ip, "user_name": user_name})

    else:
        raise MainServerError("The details are incorrect")


def is_task_conditional(user_name, task_name, return_Condition=False):
    """
    This function checks the task has a condition or not.
    :param user_name: The user who created the task
    :param task_name: The name of the task
    :param return_Condition: if "true", it will return the condition of the task, if ter is none, it will return none.
    If false, it will return True if it has a condition, false otherwise.
    :return:
    """
    if return_Condition:
        conditional = database_handler.check_for_record("Tasks", condition="Task_name=:Task_name and user_name=:user_name"
                                                                        " and Task_Conditional is not Null"
                                                     ,check_args={"Task_name": task_name, "user_name": user_name},
                                                     return_data=True, select_columns=["Task_Conditional"])
        if conditional:
            return conditional["Task_Conditional"]
        else:
            return None
    else:
        return database_handler.check_for_record("Tasks", condition= "Task_name=:Task_name and user_name=:user_name"
                                                                     " and Task_Conditional is not Null"
                                        ,check_args={"Task_name": task_name, "user_name": user_name})


def append_to_file(file_name , text, newline=True):
    file_dir = main_directory  + "/" + results_folder + "/" + file_name
    with open(file_dir, "a") as writer:
        if newline:
            writer.write(text + "\n")
        else:
            writer.write(text)


def add_results_to_files_default(user_name, task_name, results_table, work_unit):
    """
    Writes a work unit the main server got on two result files: one including the results from all the
    This function is for "defualt" tasks, that don't have a condition.
    In which case, in the "Results" file, there will be one large list, where every results from every work unit is
    appended by their id.
    :param user_name: The
    :param task_name:
    :param results_table: The name of the table that holds all the work units the main server got from this task.
    :param work_unit: a dictionary of a work unit with its results.
    """
    task_details = get_task_process_details(user_name, task_name)
    results_file , work_units_file = task_details["Task_results_file"] , task_details["work_units_file"]
    append_to_file(work_units_file, str(work_unit))

    results = work_unit["results"]
    str_results = repr(results)
    if isinstance(results, list):
        str_results = str_results[1:-1]

    if work_unit["work_unit_id"] == 1:
        append_to_file(results_file, "[" + str_results)
    else:
        append_to_file(results_file, ', ' + str_results)


def finish_results(user_name, work_server_ip, result_log):
    task_details = task_details = database_handler.find_specific_record("current_tasks",
                                                          {"server_ip": work_server_ip, "user_name": user_name},
                                                          return_data=True)
    results_file, work_units_file = task_details["Task_results_file"], task_details["work_units_file"]

    if result_log["server_status"] == TaskStatusNames.crashed.value:
        if result_log["problematic_work_unit"]["work_unit_id"] > 1:
            append_to_file(results_file, "]\n", newline=False)
        append_to_file(results_file, "This work unit crashed the code: " + str(result_log["problematic_work_unit"]))

"""  
def add_results_to_files_conditional(task_name, user_name, results_table, work_unit):

    :param task_name:
    :param user_name:
    :param results_table:
    :param work_unit: the
    :return:

    task_details = database_handler.find_specific_record("current_tasks",
                                                         {"task_name": task_name, "user_name": user_name},
                                                         return_data=True)
    results_file , work_units_file = task_details["Task_results_file"] , task_details["work_units_file"]
    append_to_file(work_units_file, work_unit)

def set_results(user_name, work_server_ip, last_consecutive_work_unit_id):

    
    id = last_consecutive_work_unit_id
    while True:
        results = database_handler.find_specific_record(results_table, {"work_unit_id": id}, return_data=True,
                                                        select_columns=["results"])
        if not results:
            return id
        id += 1
"""

def collect_results():
    """
        Collects all the results from every work_unit to one list.
        :return: a list of all the results, ordered by the work unit id.
    """
    results = []
    finished = False
    work_unit_id = 1
    while not finished:
        results_data = database_handler.find_specific_record("work_unit_results",
                                                            {"work_unit_id": work_unit_id}, return_data=True)
        if results_data is None:
            finished = True
        else:
            row_results = results_data["results"]
            if isinstance(row_results, list):
                results += row_results
            elif row_results is not None:
                results.append(results_data)

            work_unit_id += 1

    return results


def get_results(user_name, task_name):
    """

    :param user_name:
    :param task_name:
    :return:
    """
    results = get_task_process_details(user_name, task_name)
    if results is None:
        return None
    else:
        return results["Task_results"]


def free_task(task_name, user_name):
    """

    :param task_name: the name of a task.
    :param user_name: the name of the user who created the task
    """
    database_handler.update_records("current_tasks",
                                    {"server_ip": None,
                                     "Task_status": TaskStatusNames.untouched.value},
                                    condition = "Task_name=:Task_name and user_name=:user_name",
                                    code_args = {"Task_name": task_name, "user_name": user_name})


def assign_worker(user_name):
    """

    :param user_name:
    :return:
    """
    priority_current_tasks = database_handler.load_data("current_tasks",
                                                 condition="user_name=? and work_force_percentage is not null"
                                                           " and server_ip is not null",
                                                 select_args=[user_name],
                                                 order_by_columns=["work_force_percentage"], order_type="DESC",
                                                 filer_unique_row=False)



    normal_current_tasks = database_handler.load_data("current_tasks",
                                                 condition="user_name=? and work_force_percentage is null"
                                                           " and server_ip is not null",
                                                 select_args=[user_name],
                                                 order_by_columns=["Task_Connected_workers"],
                                                 filer_unique_row=False)

    if normal_current_tasks is None and priority_current_tasks is None:
        return None

    if priority_current_tasks is not None:
        #print(priority_current_tasks)
        total_work_force = database_handler.collect_sql_quarry_result(
            "select sum(Task_Connected_workers) from current_tasks where user_name=?", [user_name])[0] + 1
        for task in priority_current_tasks:
            percent_from_whole = math.ceil(total_work_force*(task["work_force_percentage"]/100.0))
            if task["Task_Connected_workers"] < percent_from_whole:
                database_handler.update_records("current_tasks",
                                                {"Task_Connected_workers": task["Task_Connected_workers"] + 1},
                                                condition="Task_name=:Task_name and user_name=:user_name",
                                                code_args={"Task_name": task["Task_name"], "user_name": user_name})


                worker_task = find_task(task["Task_name"], user_name, return_task_data=True)
                worker_task["server_ip"] = task["server_ip"]
                return worker_task



    minimum_work_force_tasks = [normal_current_tasks[0]]
    minimum_work_force = normal_current_tasks[0]["Task_Connected_workers"]
    for task in normal_current_tasks[1:]:
        if minimum_work_force == task["Task_Connected_workers"]:
            minimum_work_force_tasks.append(task)
        else:
            break

    assigned_task = choice(minimum_work_force_tasks)
    database_handler.update_records("current_tasks",
                                    {"Task_Connected_workers": assigned_task["Task_Connected_workers"] + 1},
                                    condition="Task_name= :Task_name and user_name= :user_name",
                                    code_args={"Task_name": assigned_task["Task_name"], "user_name": user_name})
    worker_task = find_task(assigned_task["Task_name"], user_name, return_task_data=True)
    worker_task["server_ip"] = assigned_task["server_ip"]
    return worker_task




#search join in sql, replace it in the future


code = """
CREATE TABLE rwr
(
	column_1 integer
		constraint rwr_pk
			primary key autoincrement,
	name varchar not null
)

"""

database_handler.create_table(code)
test_data = {'name': 'Gimmie'}

"""
insert_user('a', 'b')
insert_user('c', 'd')
insert_task('a', {"Task_name": "prime_range", "first_num": 1, "last_num": 10000, "exe_name": "prime_range"})
insert_task('a', {"Task_name": "goldbach_conjecture", "first_num": 1, "last_num": 10000, "exe_name": "gold",
                  "work_force_percentage":50})
insert_task('a', {"Task_name": "task3", "first_num": 1, "last_num": 10000, "exe_name": "gold",
                  "work_force_percentage":25})
insert_task('a', {"Task_name": "task4", "first_num": 1, "last_num": 10000, "exe_name": "gold",
                  "work_force_percentage":15})
start_working_on_a_task('a', "prime_range")
start_working_on_a_task('a', "goldbach_conjecture")
start_working_on_a_task('a', "task3")
start_working_on_a_task('a', "task4")
"""
def main():

    for table_code in tables_code:
        database_handler.create_table(table_code, replace_table=True)
    insert_user('Default_UserName', '12345')
    insert_user('c', 'd')
    insert_task('Default_UserName', {"Task_name": "prime_range", "first_num": 1, "last_num": 4000000,
                                     "exe_name": "prime_range"})
    insert_task('Default_UserName', {"Task_name": "goldbach_conjecture", "first_num": 1, "last_num": 10000,
                                     "exe_name": "gold", "work_force_percentage": 50})
    insert_task('Default_UserName', {"Task_name": "task3", "first_num": 1, "last_num": 10000, "exe_name": "gold",
                      "work_force_percentage": 25})
    insert_task('c', {"Task_name": "task4", "first_num": 1, "last_num": 10000, "exe_name": "gold"})
    insert_task('c', {"Task_name": "task5", "first_num": 1, "last_num": 10000, "exe_name": "gold",
                      "work_force_percentage":50})
    insert_task('c', {"Task_name": "task6", "first_num": 1, "last_num": 10000, "exe_name": "gold",
                      "work_force_percentage":50})
    # start_working_on_a_task('c', "task5")
    # start_working_on_a_task('c', "task6")
    # start_working_on_a_task('c', "task4")
    # assign_task("c", "task5", "10.0.0.7")
    start_working_on_a_task('Default_UserName', "prime_range")
    print(is_task_conditional("c", "task6"))
    print(database_handler.table_info("current_tasks")[0].keys())
#insert_task('a', {"Task_name": "task4", "first_num": 1, "last_num": 10000, "exe_name": "gold",
#                  "work_force_percentage":15})

#cancel_working_on_task("a", "task4")

#insert_task('c', {"Task_name": "task5", "first_num": 1, "last_num": 10000, "exe_name": "gold",
                #  "work_force_percentage":50})
#insert_task('c', {"Task_name": "task6", "first_num": 1, "last_num": 10000, "exe_name": "gold",
            #      "work_force_percentage":50})
#print(get_all_tasks_by_user('a'))
#assign_worker('a')
#print(get_number_of_workers('a'))
#print(find_user_name_by_sid('lweR-RGG_uLG5PlbdyCwAw'))
"""

"""

#if __name__ == "__main__":
main()