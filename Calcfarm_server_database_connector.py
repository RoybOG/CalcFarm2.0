import CalcFarm_Database_Analyser_2 as Db
from Calc_Farm_Essential import *


class WorkerServerError(Exception):
    """
    When an error happened in the server's actions or in his communiations with the server
    and the he can't work.
    When it is raise, it will send a negative message to the
    """
    pass


database_handler = Db.Database('work_server_database')
tables_code = []

tables_code.append("""
create table workers
(
    worker_id varchar primary key,
    worker_ip varchar,
    worker_status int default {}
);
""".format(WorkerStatusNames.just_joined.value))

tables_code.append("""
create table work_units
(
    work_unit_id integer primary key autoincrement,
    first_num int not null,
    last_num int not null,
    work_unit_status int default {},
    worker_id varchar,
    failed_count int default 0
        constraint table_name_workers_worker_id_fk
            references workers
);

""".format(WorkUnitStatusNames.untouched.value))

# database_handler.reset_table("workers")
#In the funture include a row "Last time alive), witch includes the real time where the
#Maybe even ping to the worker


#I decided to move the data of a work unit from the "Work Units" table to the "Results table",
#because when a work unit is finished, it shouldn't be in the list where the server searches for untouched work units.
#For exmaple, if there are a million work units, and the workers finished computing all of them but one,
#It won't make sense to go over all the 999,999 finished work units, just to get to the last one.

for table_code in tables_code:
    database_handler.create_table(table_code, replace_table=True)

#Make sure that the functions to this only gets id's and no full work units or worker id to prevent data from different
#sources.
#You derive all the information from the database to prevent confution and entering wrong information .
#There could be a mistake where the work unit and it's "supposed" assigned worker id, But they could be mismatched
#If you want the worker id, you dervi

def find_worker(worker_id):
    """

    :param worker_id: The id of the worker we want to check.
    :return: Information about the worker. If the worker doesn't exist, it will return None.
    """
    worker_data = database_handler.find_specific_record("workers", {"worker_id":worker_id}, return_data=True)
    return worker_data


def insert_worker(worker_details):
    """
    The function inserts a worker into the database.
    :param worker_details: a dictionary with data about the worker that will be written in the "workers" table:
            "worker_id":the id of the worker, "worker_ip": the ip of the computer the comupter is working on.
    """
    if database_handler.find_specific_record("workers",{"worker_id":worker_details["worker_id"]}):
        raise WorkerServerError("The user already exsists")
    database_handler.dump_data("workers", worker_details)


#def get_worker(worker_id):


def insert_work_unit(work_unit_details):
    """
    The function inserts a work unit into the database.
    :param work_unit_details: data about the work unit that will be written in the "work units" table.
    """

    database_handler.dump_data("work_units", work_unit_details)


def get_work_unit(work_unit_id):
    """
    Returns data about a work unit from it's database, based on the work unit's id.
    :param work_unit_id: The id of the work unit.
    :return: Returns data about the work unit. Returns None if it doesn't exist.
    """
    return database_handler.find_specific_record("work_units", {"work_unit_id": work_unit_id}, return_data=True)


def get_work_unit_by_worker(worker_id):
    """
    Returns data about a work unit from it's database, based on it worker's id.
    :param worker_id: The id of the worker that was assigned to the that work unit.
    :return: Returns the entire row about the specific work unit. Returns None if it doesn't exist.
    """
    return database_handler.find_specific_record("work_units", {"worker_id": worker_id}, return_data=True)


def get_free_work_unit():
    """
    Gets from the database a work unit that wasn't assigned to any worker and isn't worked on.
    It selects the work unit with the smallest ID, such that previous work units that were freed from workers that
    failed calculating them, will be preferred other the next work units.
    :return: a work unit as a dictionary of columns and values
    """
    smallest_id = database_handler.collect_sql_quarry_result("select min(work_unit_id) from work_units")[0]
    work_unit_data = database_handler.find_specific_record("work_units",
                                                           values={"work_unit_status":
                                                                       WorkUnitStatusNames.untouched.value,
                                                                   "work_unit_id": smallest_id}, row_num=1,
                                                           return_data=True)
    return work_unit_data


def assign_work_unit(work_unit_id, worker_id):
    """

    :param work_unit_id:
    :param worker_id:
    :return:
    """
    database_handler.update_records("work_units",
                                   {"worker_id": worker_id, "work_unit_status": WorkUnitStatusNames.in_progress.value},
                                   condition="work_unit_id=:id", code_args={"id": work_unit_id})

    database_handler.update_records("workers",{"worker_status": WorkerStatusNames.working.value},
                                   condition="worker_id=:id", code_args={"id": worker_id})


def free_work_unit(work_unit_id):
    """

    :param work_unit_id:
    :return: the amount of times the work unit was freed due to a failute in calculating it.
    If it was failed to be calculated 3 times, then it will stop working on the task and
    return tell the main server which work unit crashed the task. Otherwise.
    """
    work_unit = get_work_unit(work_unit_id)
    database_handler.update_records("work_units",
                                   {"worker_id": None, "work_unit_status": WorkUnitStatusNames.untouched.value,
                                    "failed_count": work_unit["failed_count"] + 1},
                                    condition="work_unit_id= :id", code_args={"id": work_unit_id})

    worker_id = work_unit['worker_id']
    database_handler.update_records("workers", {"worker_status": WorkerStatusNames.waiting.value},
                                   condition="worker_id= :id", code_args={"id": worker_id})

    return work_unit["failed_count"] + 1


def free_work_unit_from_worker(worker_id):
    """

    :param worker_id:
    :return:
    """
    work_unit = get_work_unit_by_worker(worker_id)
    if work_unit:
        return free_work_unit(work_unit["work_unit_id"])


def remove_worker(worker_id):
    """
    Deletes a worker who stopped running from the Work server's database.
    :param worker_id: The ID of the worker.
    """
    free_work_unit_from_worker(worker_id)
    database_handler.delete_records("workers","worker_id = ?", [worker_id])


def update_results(work_unit_id):
    """
    This function updates the database when this server sends the results of a work unit to the main server.
    It deletes the work units
    :param work_unit_id: The ID of a work unit to be transferred to the "Results" table.
    """

    database_handler.delete_records("work_units", "work_unit_id=?", con_args=[work_unit_id])
    database_handler.update_records("workers", {"worker_status": WorkerStatusNames.waiting.value},
                                   condition="worker_id= :id", code_args={"id": work_unit_id})
