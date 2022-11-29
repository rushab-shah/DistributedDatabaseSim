# Author: Rushab Shah
# Driver code
# This code is responsible to parse input and send parsed data to TM

import sys
from transaction_manager import TransactionManager

# Two Scenarios: File Input/ command line input
tm_obj = TransactionManager()

def main():
    if(len(sys.argv)<2):
        parse_from_cmd()
    else:
        parse_from_file(sys.argv[1])

def parse_from_file(file_path):
    try:
        print("File "+file_path)
        input_file = open(file_path,'r')
        for line in input_file:
            tm_obj.process(line)
        print("Read complete")
        input_file.close()
    except FileNotFoundError:
        print("Invalid File Path")

def parse_from_cmd():
    print("cmd")
    input_op = input()
    while(input_op!="exit"):
        if(validate_cmd_input(input_op)):
            tm_obj.process(input_op)
        else:
            print("Enter valid operation\n")
        input_op = input()


def validate_cmd_input(line):
    # TODO Add validation for input ops from cmd
    return True

if __name__ == "__main__":
    main()