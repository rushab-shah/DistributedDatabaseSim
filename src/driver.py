# Author: Rushab Shah
# Driver code
# This code is responsible to parse input and send parsed data to TM

import sys
from transaction_manager import TransactionManager

# Two Scenarios: File Input/ command line input

def main():
    tm_obj = TransactionManager()
    tm_obj.hello()

if __name__ == "__main__":
    main()