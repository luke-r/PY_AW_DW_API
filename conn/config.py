import pyodbc as py
import socket as st
import numpy as np
import pandas as pd
import sqlalchemy as sa

#utilizes pyodbc
class dbconn:
    def qryconfig(**kwargs):
        if("name" in kwargs and "dbname" in kwargs):
            raise ValueError("Pass either a name (informal) or a database name.")
        elif("name" in kwargs):
            where = """NAME = '""" + kwargs['name'] + """'"""
        elif("dbname" in kwargs):
            where = """DATABASE_NAME = '""" + kwargs['dbname'] + """'"""
        else:
            raise ValueError("Only pass one name or database name at a time. Not both!")
        cnxn = py.connect(
                r'DRIVER={ODBC Driver 13 for SQL Server};'
                r'SERVER=LOOK\LR;'
                r'DATABASE=KELPIE_CONFIG;'
                r'Trusted_Connection=yes;'
            )
        sql = """SELECT  
        [PYTHON_CONNECTION_CONFIG_SK]
        ,[NAME]
        ,[SERVER_NAME]
        ,[DATABASE_NAME]
        ,[DRIVER]
        ,[ENVIRONMENT] from [dbo].[T_PYTHON_CONNECTION_CONFIG]
        WHERE """ + str(where)
        data = pd.read_sql(sql,cnxn)
        return data
    
    def getenv():
        i = st.gethostname()
        env = dbconn.qryconfig(name='python')
        try:
            env = str(env.loc[env['SERVER_NAME'] == i]['ENVIRONMENT'][0])
        except IndexError:
            print("Your python environment hasn't been entered correctly in the config table, or you are trying to run this script from your local machine.")
            env = None
        return env

    def getcon(**kwargs):
        if("name" in kwargs and "dbname" in kwargs):
            raise ValueError("Pass either a name or a dbname. Not both!")
        elif("name" in kwargs):
            con = dbconn.qryconfig(name=kwargs['name'])
        elif("dbname" in kwargs):
            con = dbconn.qryconfig(dbname=kwargs['dbname'])
        con = con.loc[con['ENVIRONMENT'] == dbconn.getenv()].iloc[0]
        cnxn = py.connect(
        r'DRIVER=' + con['DRIVER'] + ';'
        r'SERVER=' + con['SERVER_NAME'] + ';'
        r'DATABASE=' + con['DATABASE_NAME'] + ';'
        r'Trusted_Connection=yes;'
        )
        if len(con) == 0:
            raise ValueError("Database not found with the specified name for the environment your script is running in.")
        else:
            return cnxn
    def getcon_cowboy_con(env,**kwargs):
        if("name" in kwargs and "dbname" in kwargs):
            raise ValueError("Pass either a name or a dbname. Not both!")
        elif("name" in kwargs):
            con = dbconn.qryconfig(name=kwargs['name'])
        elif("dbname" in kwargs):
            con = dbconn.qryconfig(dbname=kwargs['dbname'])
        con = con.loc[con['ENVIRONMENT'] == env].iloc[0]
        cnxn = py.connect(
        r'DRIVER=' + con['DRIVER'] + ';'
        r'SERVER=' + con['SERVER_NAME'] + ';'
        r'DATABASE=' + con['DATABASE_NAME'] + ';'
        r'Trusted_Connection=yes;'
        )
        if len(con) == 0:
            raise ValueError("Database not found with the specified name for the environment your script is running in.")
        else:
            return cnxn

#utilizes sqlalchemy            
class dbconn2:
    def qryconfig(**kwargs):
        if("name" in kwargs and "dbname" in kwargs):
            raise ValueError("Pass either a name (informal) or a database name.")
        elif("name" in kwargs):
            where = """NAME = '""" + kwargs['name'] + """'"""
        elif("dbname" in kwargs):
            where = """DATABASE_NAME = '""" + kwargs['dbname'] + """'"""
        else:
            raise ValueError("Only pass one name or database name at a time. Not both!")
        cnxn = py.connect(
                r'DRIVER={ODBC Driver 13 for SQL Server};'
                r'SERVER=LOOK\LR;'
                r'DATABASE=KELPIE_CONFIG;'
                r'Trusted_Connection=yes;'
            )
        sql = """SELECT  
        [PYTHON_CONNECTION_CONFIG_SK]
        ,[NAME]
        ,[SERVER_NAME]
        ,[DATABASE_NAME]
        ,[DRIVER]
        ,[ENVIRONMENT] from [dbo].[T_PYTHON_CONNECTION_CONFIG]
        WHERE """ + str(where)
        data = pd.read_sql(sql,cnxn)
        return data
    
    def getenv():
        i = st.gethostname()
        env = dbconn2.qryconfig(name='python')
        try:
            env = str(env.loc[env['SERVER_NAME'] == i]['ENVIRONMENT'][0])
        except IndexError:
            print("Your python environment hasn't been entered correctly in the config table, or you are trying to run this script from your local machine.")
            env = None
        return env
        
    def getcon(**kwargs):
        if("name" in kwargs and "dbname" in kwargs):
            raise ValueError("Pass either a name or a dbname. Not both!")
        elif("name" in kwargs):
            con = dbconn2.qryconfig(name=kwargs['name'])
        elif("dbname" in kwargs):
            con = dbconn2.qryconfig(dbname=kwargs['dbname'])
        con = con.loc[con['ENVIRONMENT'] == dbconn.getenv()].iloc[0]
        engine = sa.create_engine('mssql+pyodbc://' + con['SERVER_NAME'] + '/' + con['DATABASE_NAME'] + '?driver=SQL+Server')
        if len(con) == 0:
            raise ValueError("Database not found with the specified name for the environment your script is running in.")
        else:
            return engine
        
    def getcon_cowboy_con(env,**kwargs):
        if("name" in kwargs and "dbname" in kwargs):
            raise ValueError("Pass either a name or a dbname. Not both!")
        elif("name" in kwargs):
            con = dbconn2.qryconfig(name=kwargs['name'])
        elif("dbname" in kwargs):
            con = dbconn2.qryconfig(dbname=kwargs['dbname'])
        con = con.loc[con['ENVIRONMENT'] == env].iloc[0]
        engine = sa.create_engine('mssql+pyodbc://' + con['SERVER_NAME'] + '/' + con['DATABASE_NAME'] + '?driver=SQL+Server')
        if len(con) == 0:
            raise ValueError("Database not found with the specified name for the environment your script is running in.")
        else:
            return engine
            
    def runtrans(engine,sql):
        if isinstance(sql,str):
            sql = [sql]
        connection = engine.connect()
        trans = connection.begin()

        trans_return = []

        try:
            for elem in sql:
                trans_sub = connection.execute(elem).fetchall()
                trans_return.append(trans_sub)
            trans.commit()
        except:
            trans.rollback()
            raise ValueError("Encountered issues running sql or committing transactions. Transactions rolled back.")
        return trans_return

    def runtrans_apioutput(engine,sql,keylabel):
        connection = engine.connect()
        trans = connection.begin()

        try:
            trans_sub = connection.execute(sql).fetchall()
            d, a = {}, []
            for rowproxy in trans_sub:
                for column, value in rowproxy.items():
                    d = {**d, **{column: value}}
                a.append(d)
            new_dict = {item[keylabel]: item for item in a}
            trans.commit()
        except:
            trans.rollback()
            raise ValueError("Encountered issues running sql or committing transactions. Transactions rolled back.")
        return new_dict

class dborm:
    def __init__():
        return None