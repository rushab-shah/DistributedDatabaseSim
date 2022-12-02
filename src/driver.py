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
        # print("File "+file_path)
        input_file = open(file_path,'r')
        for line in input_file:
            if(check_for_unnecessary_lines(line)):
                tm_obj.opProcess(line)
        # while(tm_obj.operations_left()):
        #     ## Wait till all operations done
        #     .
        
        print("\nCompleted Processing")
        input_file.close()
    except FileNotFoundError:
        print("Invalid File Path")

def parse_from_cmd():
    print("cmd")
    input_op = input()
    while(input_op!="exit"):
        if(check_for_unnecessary_lines(input_op)):
            tm_obj.opProcess(input_op)
        else:
            print("Enter valid operation\n")
        input_op = input()


def check_for_unnecessary_lines(line):
    # TODO Add validation for input ops from cmd
    if(line.startswith("//")) or line=="" or line.strip()=="":
        return False
    return True

if __name__ == "__main__":
    main()