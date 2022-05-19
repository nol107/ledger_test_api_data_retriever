## GET STARTED

# Prerequisites

- This repository was implemented with python 3.8.5
- Before running the main function, run a "pip install -r requirements.txt" to get all necessary additional libraries (to avoid libs conflicts, you may want to setup a virtualenv to run the code)

# How it works

- In the root of the repository, create a "config_api.ini" file (based on the config_template.ini one) : you have to add the JWT token and the API endpoint.
- You can change the other config default values if it suits you (default_start_date / default_path)
- Place yourself in the "data_updater" repository
- Run "python3 entrypoint.py" (or "python entrypoint.py" according to your env) to generate a csv file with all the data available on the endpoint
- If you re-run the same command, it will not append any data to the file.
- If you delete the final rows of the csv file and re-run the command, it will only append the rows you deleted (or the new transactions added to the endpoint)
- You can also run "python3 entrypoint.py --path my_personal_filename.csv" if you want to override the default file path

# Next steps

- Add unit tests (with pytest and requests-mock for example)
- Add a docker and Airflow setup to schedule the refresh of the data
