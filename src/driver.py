# Author: Rushab Shah
# Driver code
# This code is responsible to parse input and send parsed data to TM

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
        parse_from_cmd()
    elif(len(sys.argv)==2 and debug):
        parse_from_cmd()
    else:
        if(debug):
            parse_from_file(sys.argv[2])
        else:
            parse_from_file(sys.argv[1])

def parse_from_file(file_path):
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
        
        print("\nCompleted Processing")
        input_file.close()
    except FileNotFoundError:
        print("Invalid File Path")

def parse_from_cmd():
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
    # TODO Add validation for input ops from cmd
    if(line.startswith("//")) or line=="" or line.strip()=="":
        return False
    return True

if __name__ == "__main__":
    main()