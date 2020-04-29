import requests
import bottle
import time
import json
import enum
import sys
TIMEOUT = 5


class HttpErrorDetails:
    def __init__(self, request_code, request_name, request_message, try_again=False):
        """
        This object holds the details of an http error and how to handle it.
        :param request_code: The http error number.
        :param request_name: The name of the http error
        :parm request_message:The message that explains the error.
        It will display when the corresponding error will be raised.
        :param try_again: It tells you when this error occur, whether you want the client to try again or to shut down.
        True = try again
        False = crash
        """
        self._request_code = request_code
        self._request_name = request_name
        self._request_message = request_message
        self._try_again = try_again

    def request_code(self):
        return self._request_code

    @property
    def request_name(self):
        return self._request_name

    @property
    def request_message(self):
        return self._request_message

    @property
    def try_again(self):
        return self._try_again

    def __str__(self):
        return "HTTP error code no' {}: {}. \n {}".format(self._request_code, self._request_name, self._request_message)


HTTP_ERRORS_DATA = {
    400: HttpErrorDetails(400, "Bad Request", "The server did not recognise the command"),
    401: HttpErrorDetails(401, "Unauthorized", "The server doesn't recognise you."),
    403: HttpErrorDetails(403, "Forbidden", "You aren't qualified to get access to this service"),
    404: HttpErrorDetails(404, "Not Found", "The resource you asked for doesn't exist.", True),
    418: HttpErrorDetails(418, "Tea Pot", "I'm sorry kind sir, I can't offer you tea!"),
    500: HttpErrorDetails(500, "Internal Server Error", "The server crashed from an error in the code.")
}


class RepeatError(Exception):
    """
    This error is raised if the inner function has crashed from an expection, but it wants to try again.
    It captures the error and only after a few attempts, it lets go.
    """
    def __init__(self, error):
        """
        :param error:The error/exception that caused a problem in the inner program.
        """
        self.error = error


class Repeat:
    """
        This decoder runs a function with a special fact: If the inner function crashes but wants to try again, then it
        tells the decoder to try again until it succeds or until it tried too many times.
        If It didn't succeed, it will raise the error that crashed the inner function.
    """

    num_of_attempts = 3
    # The number of attempts until the decoder will give up and raise the error.

    def __init__(self, func):
        self._func = func

    def __call__(self, *args, **kwargs):
        inner_error = None
        try:
            return self._func(*args, **kwargs)
        except RepeatError as e:
            print("{} crashed, trying again.".format(self._func.__name__))
            inner_error = e.error
            print(inner_error)

        for i in range(2, Repeat.num_of_attempts + 1):
            time.sleep(3)
            print("Trying again")
            try:
                return self._func(*args, **kwargs)
            except RepeatError as e:
                print("tried running again {}, failed {} times.".format(self._func.__name__, i))
                inner_error = e.error
                print(inner_error)
        else:
            print("As much as it tried, it couldn't fix the error.")
            raise inner_error


class CommunicationError(Exception):
    """
    If there was an error in the communication of the client to the server,
    or if the protocols of communication were breached, this error will appear.
    """
    def __init__(self, error_message):
        self._error_message = str(error_message)

    def __str__(self):
        """
        :return: the error message.
        """
        return self._error_message


class HttpError(CommunicationError):
    """
    This error handles an http_error by it's code number.
    """

    def __init__(self, http_code):
        if http_code in HTTP_ERRORS_DATA:
            self.error = HTTP_ERRORS_DATA[http_code]
        else:
            self.error = HttpErrorDetails(http_code, "unknown HTTP ERROR", "The program doesn't recognise this error."
                                                                           " Try and search this type of http error.")

    def __str__(self):
        """
        :return: the error message.
        """
        return str(self.error)

    def raise_error(self, failed=False):
        """
        This function allows control other the raising. If "try again" is true
        and "Connect_to_route" wants to try again tis try of http error, then it will not raise itself.
        :param failed: If the client tried 3 times to communicate with the function and failed, then it should
        raise the exception.
        True- It failed 3 times, crash the client!
        False-Try again!
        """
        if failed or not self.error.try_again:
            raise self


def package_data(data_dict):
    """
        This function packages data to a Json object to send to the server.
        :param data_dict: dictionary that contains data to send to the client
        :return: a json object version of the data
    """
    return json.dumps(data_dict)


@Repeat
def connect_to_route(route_url, server_ip, input_list=None):
    """
    This function sends a 'get' request to one of the routes on a server from the client
    :param route_url: a string of the url of the route
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    :param server_ip: The IP of the server the client wants to communicate with.

    You need to make sure that the slashes of the url are "/" and not "\"!
    :return: the output from the server- an "requests" response object that you can read its content or convert to a
    dictionary from a JSON object.
    """

    url_to_server = server_ip + '/' + route_url
    # The server port has its own slashes and they will be removed by the function and make an invalid url
    if input_list:
        for worker_input in input_list:
            if worker_input:
                url_to_server = url_to_server + '/' + str(worker_input)

    resp = None
    try:
        resp = requests.get(url=url_to_server, timeout=TIMEOUT)
        # "resp" is an requests object that holds the response to the request
        resp.raise_for_status()
        try:
            resp_content = resp.json()
        except ValueError:
            resp_content = resp.content

        return resp_content
        # else:
        #    inner_error = CommunicationError("The server returned nothing")
        #    raise RepeatError(inner_error)

    except requests.exceptions.HTTPError:
        inner_error = HttpError(resp.status_code)
        inner_error.raise_error()
        raise RepeatError(inner_error)

    except requests.exceptions.ConnectTimeout:
        inner_error = CommunicationError("The server didn't respond")
        raise RepeatError(inner_error)
    except (requests.exceptions.RequestException, ConnectionError) as e:
        inner_error = CommunicationError('There was an error in communication: \n' + str(e))
        raise RepeatError(inner_error)


@Repeat
def post_to_route(route_url, data, server_ip, input_list=None):
    """
        This function sends 'post' request to one of the routes on the server
        :param route_url: a string of the url of the route
        :param data: the data that the client wants to send to the server. If the data is a dictionary, it will be
        sent as an JSON object.
        that is too big to be sent normally as an argument.
        :param server_ip: The IP of the server the client wants to communicate with.
        :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
        of the route's arguments)
        :return: the output from the route in the form of a dictionary that was a JSON object.
    """

    url_to_server = server_ip + '/' + route_url
    if input_list:
        for worker_input in input_list:
            if worker_input:
                url_to_server = url_to_server + '/' + str(worker_input)

    if isinstance(data, dict):
        data = package_data(data)

    resp = None
    try:
        resp = requests.post(url=url_to_server, data={"json": data})
        resp.raise_for_status()
        return resp
    except requests.exceptions.HTTPError:
        inner_error = HttpError(resp.status_code)
        inner_error.raise_error()
        raise RepeatError(inner_error)

    except requests.exceptions.ConnectTimeout:
        inner_error = CommunicationError("The server doesn't respond")
        raise RepeatError(inner_error)
    except (requests.exceptions.RequestException, ConnectionError) as e:
        inner_error = CommunicationError('There was an error in communication: \n' + str(e))
        raise RepeatError(inner_error)

def Recieve_information_from_client():
    """
    As a part of the communication protocol of Calcfarm, the client posts data to the server in a json.
    :return:
    """
    data = bottle.request.forms.get('json')
    if data:
        return json.loads(data)
    else:
        raise CommunicationError("The server returned nothing")
"""
#This function will be in the worker's class
def recieve_data_from_server(route_url, input_list=None, server_ip=ServerURL, decode_to_json=True):

    The worker gets data from a route on the server according to it's protocol with the server.

    as a JSON file according to it protocol, it's first agument in the URL must be it's Worker ID(If it has at this
    point), so the server can recognise it.
    Another thing is that most of the data between the workers and the servers is sent as a JSON object.


    :param route_url: the url where the wanted route is
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :param server_ip: The Main server or the Work Server.
    Firstly It will first connect to the main server to get the ip of the work server it got attached to.
    Afterwards, it will connect to that work server to get work units.
    :return: the output from the route in the form of a dictionary that was a JSON file



    if ID is not None:
        input_list = [ID] + input_list

    raw_data = connect_to_route(route_url, server_ip, input_list)
    if raw_data is None:
        return None
    else:
        if decode_to_JSON:
            return raw_data.json()
        else:
            return raw_data


def send_to_server(route_url, data_dict, input_list=None):

     This function sends 'post' request to one of the routes on the server
    :param route_url: a string of the url of the route
    :param data_dict: the data that the worker wants to send to the server in the form of a dictionary
    that is too big to be sent normally as an argument.
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :return: the output from the route in the form of a dictionary that was a JSON file

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
        raise WorkerError("HTTP error of " + str(req.status_code) + "- " + HTTPSTATUSCODES[req.status_code]
                          + ': \n' + HTTPSTATUSMESSAGES[req.status_code])
    except requests.exceptions.ConnectTimeout:
        raise WorkerError("The server didn't respond")
    except requests.exceptions.RequestException as e:
        raise WorkerError('There was an error in communication: ' + str(e))


def connect_to_route(route_url, server_ip, input_list=None):

    This function sends a 'get' request to one of the routes on the server
    :param route_url: a string of the url of the route
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :return: the output from the server- a JSON file the was converted to a dictionary.
    (all the clients to the server communicate to it using JSON files as a part of the protocol)

    print(ServerURL)
    if input_list is None:
        input_list = []

    if ID is not None:
        input_list = [ID] + input_list

    url_to_server = server_ip + '/' + route_url
    #The server port has its own slashes and they will be removed by the function and make an invalid url
    for worker_input in input_list:
        if len(str(worker_input)) > 0:
            url_to_server = url_to_server + '/' + str(worker_input)

    req = None
    try:
        req = requests.get(url=url_to_server, timeout=TIMEOUT)
        # if 'text' in str(req.headers.get('content-type').lower())
        # or 'html' in str(req.headers.get('content-type').lower()):
        #print(req.content)

        req.raise_for_status()
        return req
    except requests.exceptions.HTTPError:
        raise WorkerError("HTTP error of " + str(req.status_code) + "- " + HTTPSTATUSCODES[req.status_code]
                          + ': \n' + HTTPSTATUSMESSAGES[req.status_code])
    except requests.exceptions.ConnectTimeout:
        raise WorkerError("The server didn't respond")
    except requests.exceptions.RequestException as e:
        raise WorkerError('There was an error in communication: ' + str(e))


def recieve_data_from_server(route_url, input_list=None, server_ip=ServerURL):

    The worker gets data from a route on the server as a JSON file according to it protocol

    :param route_url: the url where the wanted route is
    :param input_list: a list of all the inputs to the route(the number of inputs need to match the number
    of the route's arguments)
    :param server_ip: The Main server or the Work Server.
    Firstly It will first connect to the main server to get the ip of the work server it got attached to.
    Afterwards, it will connect to that work server to get work units.
    :return: the output from the route in the form of a dictionary that was a JSON file



    if ID is not None:
        input_list = [ID] + input_list

    raw_data = connect_to_route(route_url, server_ip, input_list)
    if raw_data is None:
        return None
    else:
        return raw_data.json()


def create_folder(folder_name):
    folder_path = handle_path(main_directory + '/' + folder_name)
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)"""
""""""

# Communication Protocol


class WorkServerStatusNames(enum.Enum):
    no_work = 0
    has_work = 1
    finished_work = 2


class WorkUnitStatusNames(enum.Enum):
    untouched = 0
    in_progress = 1


class WorkUnitCalculationStatusNames(enum.Enum):
    calculated = 1
    failed = -1
    crashed = -2


class WorkerStatusNames(enum.Enum):
    just_joined = 0
    waiting = 1
    working = 2
    shut_down = 3


class TaskStatusNames(enum.Enum):
    untouched = 0
    in_progress = 1
    finished = 2
    failed = -1
    crashed = -2
