## Precios Claros data downloader
A set of Python scripts to download http://preciosclaros.gob.ar Argentina's markets 
and supermarkets prices data to a SQLite database.

## Important

This code is working today (February 26, 2018) but probably almost any further 
changes to the API will broke it.

If you decide to adapt to make it work to any new API version make a Pull Request 
with your adaptations, will be greatly appreciated.

If you have any kind of doubts or suggestions don't hesitate to create an issue.

## Installation

1. Clone this repo and get into
  ```sh
  git clone https://github.com/martjanz/precios-claros && cd precios-claros
  ```
2. (optional) Create a Python virtual environment, then activate it
  ```sh
  virtualenv venv && source venv/bin/activate
  ```
3. Install Python dependencies
  ```sh
  pip install -r requirements.txt
  ```

## Run
  ```sh
  ./scrap.sh
  ```

The result will be a SQLite database called `data.sqlite` where all data will be stored.
