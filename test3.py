import time
import bottle
import requests


def long_post(route_url, data, server_ip, input_list=None, num_of_fails=0):
    """
    This function is for sending posts that are longer then the limit.
    :param route_url:
    :param data:
    :param server_ip:
    :param input_list:
    :param num_of_fails:
    """

    post_limit = bottle.BaseRequest.MEMFILE_MAX

num_of_sets =0
for j in range(1,14):
    print("set " + str(j))
    for i in range(j, 0, -1):
        print(j)
        num_of_sets =num_of_sets + i
print(num_of_sets)