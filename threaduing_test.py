import threading
import inspect
import CalcFarm_Database_Analyser_2 as db
import time
import logging
import sys



db_con = db.Database("threading_db")
def set_up_db():
    code = """
    create table threads
(
	thread_name varchar
		constraint threads_pk
			primary key,
	last_time_alive int default (cast(strftime('%s','now') as int))
);
    """
    db_con.create_table(code, replace_table=True)

class myThread(threading.Thread):
    db_lock = threading.Lock()

    def _set_up_log(self):
        self.thread_logger = logging.getLogger(self.name)
        self.thread_logger.setLevel(logging.DEBUG)
        thread_handler = logging.StreamHandler()
        thread_handler.setLevel(logging.DEBUG)
        thread_logger_formatter = logging.Formatter("%(name)s:%(asctime)s:%(levelname)s:%(message)s")
        thread_handler.setFormatter(thread_logger_formatter)
        self.thread_logger.addHandler(thread_handler)

    def _thread_log(self, message, message_level):
        with myThread.db_lock:
            self.thread_logger.log(message_level, message)

    def __init__(self, name, target_name, target_args=None):
        threading.Thread.__init__(self, target=self.thread_setup, name=name)
        self.daemon = True
        self._set_up_log()
        self._name = name
        self._target_name = target_name
        self._target_args = target_args
        self._indent = None
        self._thread_db_con = None

    def thread_setup(self):
        self._thread_db_con = db.Database("threading_db")
        self._indent = threading.get_ident()
        if not self._name:
            self._name = threading.currentThread().getName()
        self._thread_log("Starting thread: " + self._name, logging.INFO)
        self._target_name(*self._target_args)

    @property
    def indent(self):
        return self._indent

    @property
    def name(self):
        return self._name


class worker(myThread):

    def __init__(self, name, d):
        super().__init__(name, self.delay, (d,))
        with myThread.db_lock:
            db_con.dump_data("threads", {"thread_name": self._name})

    def delay(self, d):
        self._thread_db_con = db.Database("threading_db")
        while True:
            self._thread_log(self._name + " is still alive", logging.DEBUG)
            self.write_log_in_db()
            time.sleep(d)

    def write_log_in_db(self):
        with myThread.db_lock:
            # problem is the curser sql object is in the main thread and they can be used only in the same thread.
            # So I must create an sql object he can create with.
            self._thread_db_con.execute_sql_code(
                "update threads SET last_time_alive=(cast(strftime('%s','now') as int)) "
                "where thread_name=?", [self.name])


class manager(myThread):
    def __init__(self, name, num_of_workers):
        super().__init__(name, self.check_threads)
        self.num_of_workers = num_of_workers
        self.thread_list = None

    def create_workers(self):
        self.thread_list = []
        for i in range(self.num_of_workers):
            t = worker("t" + str(i), 3)
            self.thread_list.append(t)

    def start_workers(self):
        for thread_obj in self.thread_list:
            thread_obj.start()

    def check_threads(self):
        while True:
            for thread_obj in self.thread_list:
                frame = sys._current_frames().get(thread_obj.ident, None)
                print(frame.f_code.co_firstlineno)
            time.sleep(10)


set_up_db()
m1 = manager("m1", 5)
m1.create_workers()
m1.start_workers()
m1.check_threads()

#t1.delay(1)
#d2 = threading.Thread(target=d, name="d")
time.sleep(24)

