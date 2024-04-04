"""
| Â© 2020 Eaton Corporation. All rights reserved.

| Function:  Handle all the process related to SQL
| Author: AditeeBapat@eaton.com
| Created On: 10/11/2020
| Modified on:

| Trigger:
"""

# ***** Load modules *****
import pandas as pd
import pyodbc


# ***** Define SQL Class *****
class ProSQL():
    """Process SQL Data."""

    def __init__(self, dict_const):
        # Credentials
        self.__dict__['sql_user'] = dict_const['sql_user']
        self.__dict__['sql_pass'] = dict_const['sql_pass']

        # SQL Constants
        self.__dict__['url'] = dict_const['sql_url']
        self.__dict__['database'] = dict_const['sql_database1']
        self.__dict__['db'] = dict_const['sql_db']

    def read_sql_table(self, tbl_name, filt_col='', filt_val='', ls_col="",
                       ls_rename=""):
        """
        Read table from SQL database.

        Error Handling: Returns empty pandas data frame if case of issues.

        Parameters
        ----------
        tbl_name : Name of SQL table to be read
        filt_col : optional arguments. name of column to be filtered
        filt_val : optional arguments. value on which column to be filtered
        ls_col_org : optional arguments. List of columns of interest.
        ls_col_new : optional arguments. List of columns to be used.

        Returns
        -------
        pandas data frame
        """
        # *** Code Constants ***
        sql_url, sql_database = self.url, self.database
        sql_db = self.db
        sql_user, sql_pass = self.sql_user, self.sql_pass

        # *** Setup SQL connection ***
        # make an odbc connection to javelin sql db
        sql_conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                                  f'Server={sql_url}'
                                  f'Database={sql_database};Uid={sql_user};Pwd={sql_pass};'
                                  f'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')

        # *** Check if table to be read exists ***
        sql_string = "SELECT * FROM information_schema.tables"
        df_existing_tables = pd.read_sql(sql_string, sql_conn)
        flag_exists = tbl_name in list(df_existing_tables['TABLE_NAME'])

        if flag_exists:
            # *** Build Query ***
            # Query : Columns to read
            if ls_col == "":
                ls_col = "*"
            else:
                ls_col = ','.join(map(str, ls_col))

            sql_query_text = list()
            sql_query_text.append("SELECT %s FROM %s " % (ls_col, tbl_name))

            # Query : Filter on SQL
            if filt_col != "":
                t_text = f" where {sql_db}.{tbl_name}.{filt_col}='{filt_val}'"
                sql_query_text.append(t_text)

            # Query : Final
            sql_query_text.append(";")
            sql_query_text = "".join(sql_query_text)

            # ***** Read Data *****
            tbl_data = pd.read_sql(sql_query_text, sql_conn)

            # *** Rename Columns ***
            if ls_rename != "":
                tbl_data.columns = ls_rename

        else:
            # Return empty pandas data frame if the table doe not exist
            tbl_data = pd.DataFrame()

        return tbl_data
