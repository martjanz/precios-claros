#!/bin/sh

while
    #statements
    python scrap.py
    returncode=$?

    if [ $returncode > 0 ]; then
        echo "Ocurrió algún error. Reiniciando script..."
    fi

    # check while condition
    [ $returncode > 0 ]
do :; done
