import subprocess
import time
import os


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


def python_executor(script_name, args=None, num_of_fails=0):
    """
    This function will execute the python file.
    There are 3 data streams to the program called 'standard streams' that the program communicates with
    to it's environment: the "standard input", the "standard error" and
    the "standard output", which is where the program places its output.
    when you "print" in a program, you are putting it on the output stream, that is set to the console by default.
    The program sets the "standard output" to the "subprocess" module, and then returns it to the program.
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
        get_file(script_name)
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
                return python_executor(script_name, args=args, num_of_fails=num_of_fails)

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
            return python_executor(script_name, args=args, num_of_fails=num_of_fails)

        # If the program couldn't run the script, it wasn't downloaded fully or is corrupted
        # (Assuming that there is no bug in the script code that crashed it).
        # The program will try and reload the file to fix the issue.



