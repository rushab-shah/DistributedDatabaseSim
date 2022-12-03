## Author: Rushab Shah, Deepali Chugh
## File: driver.py
## Date: 12/03/2022
## Purpose: This is a driver code meant to internally call our Transaction Manager and simulate transactions
##          The driver code has an optional file input. If file input isn't provided then input operations are taken
##          from command line input. The program terminates when user enters "exit" (in case of command line i/p)
##          Additionally, we have a "-d" option, which is used by the developer for debugging in case of errors.
##          This option doesn't affect program execution and is simply used for debugging the system.
##
## NOTE: The debug option is not needed to view program output statements. The debug option prints additional logic
##       specific statements
##
## The driver code is the first point of execution for this project.
## Usage: python3.6 driver.py <path_to_input_file>                      [This is for file input scenario ]
## Usage: python3.6 driver.py                                           [This is for command line scenario ]
## Usage: python3.6 driver.py -d <path_to_input_file>                   [Usage with debug option on]
## Usage: python3.6 driver.py -d                                        [Usage with debug option on]

import sys, getopt
from transaction_manager import TransactionManager

options = "d"
debug = False
tm_obj = TransactionManager()
# Two Scenarios: File Input/ command line input
def main():
    debug = False
    try:
        args, vals = getopt.getopt(sys.argv[1:],options)
        for arg, val in args:
            if arg in '-d':
                print("Debugger on")
                debug = True
                tm_obj.toggle_debugger(debug)

    except getopt.error:
        print(getopt.error)
    
    if(len(sys.argv)<2):
        parse_from_cmd(debug)
    elif(len(sys.argv)==2 and debug):
        parse_from_cmd(debug)
    else:
        if(debug):
            parse_from_file(sys.argv[2],debug)
        else:
            parse_from_file(sys.argv[1],debug)

def parse_from_file(file_path, debug):
    try:
        # print("File "+file_path)
        input_file = open(file_path,'r')
        for line in input_file:
            if(check_for_unnecessary_lines(line)):
                tm_obj.opProcess(line)
        # print("##### Checking if ops pending")
        while(tm_obj.operations_left()):
            ## Wait till all operations done
            if debug:
                print("Handling remaining ops")
            tm_obj.finish_remaining_operations()
            break
        
        if debug:
            print("\nCompleted Processing")
        input_file.close()
    except FileNotFoundError:
        print("Invalid File Path")

def parse_from_cmd(debug):
    input_op = input()
    while(input_op!="exit"):
        if(check_for_unnecessary_lines(input_op)):
            tm_obj.opProcess(input_op)
        else:
            print("Enter valid operation\n")
        input_op = input()
    if input_op=="exit":
        if debug:
            print("Finishing pending ops")
        tm_obj.finish_remaining_operations()


def check_for_unnecessary_lines(line):
    if(line.startswith("//")) or line=="" or line.strip()=="":
        return False
    return True

if __name__ == "__main__":
    main()