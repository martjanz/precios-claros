#!/bin/bash

# Define a timestamp function
timestamp() {
  date +"%T"
}

source venv/bin/activate

while
    #statements
    python scrap.py
    returncode=$?

    if [ $returncode -ne 0 ]; then
        echo "Ocurrió algún error. Reiniciando script..."
    fi

    # check while condition
    [ $returncode -ne 0 ]
do :; done

timestamp
