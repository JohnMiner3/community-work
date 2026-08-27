import pyodbc

# Get the list of all installed ODBC drivers
driver_list = pyodbc.drivers()

if driver_list:
    print("Locally installed ODBC drivers:")
    for driver in driver_list:
        print(f"- {driver}")
else:
    print("No ODBC drivers found or pyodbc could not access the driver manager.")
