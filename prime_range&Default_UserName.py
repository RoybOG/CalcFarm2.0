from sys import argv
import time
ARG = argv[1:]

"""
This program will find all the prime numbers in the range of numbers given.
The algorithem of this program resembles the Sieve of Eratosthenes that removes from a range of number all the numbers
that are multiples of 3, then multiples of 5 and so on...
"""


def customFilter(i, num):
    return (num==i or i%num>0)


def Sieve_of_Eratosthenes(firstnum, lastnum):
    """

    :param firstnum:the first number in the number range
    :param lastnum: the last number
    :return:
    """

    """
    This is a filter that the user can enter, such that the exe file will only run on certien numbers that meet
    certain criteria.
    The program will only check numbers that pass this filter.
    :return: True if the number qualifies for the research, false otherwise.
    """
    if(firstnum<2):
        firstnum = 2
    numrange = range(firstnum, lastnum + 1)
    #print(str(list(numrange)))
    #print(str(list(range(3,int(lastnum**0.5)+1, 2))))
    numrange = [j for j in numrange if customFilter(j, 2)]
    #It checks if the numbers in the range divide only with odd numbers becuase the even numbers aren't prime
    for num in range(3,int(lastnum**0.5)+1, 2):
        #It checks until the square root of the biggest number in the range becuase all of his factors are smaller
        #equal to his square root(proof in a seperate document)
        numrange = [i for i in numrange if customFilter(i, num)]
    return numrange

def main():
    #print(str(ARG))
    #print(FIRSTNUM)

    SOLUTIONS = []
    firstnum = int(ARG[0])  # int(ARG[0]) #
    lastnum= int(ARG[1])
    print(repr(Sieve_of_Eratosthenes(firstnum, lastnum)))
    #print(str(SOLUTIONS))

if __name__ == '__main__':
   main()