import configparser
import pymysql
import pymysql.cursors

class MySQLConnection:
    config_file: str
    host: str
    username: str
    password: str
    charset: str
    database: str
    con: pymysql.connections

    def __init__(self, config_file: str):
        self.config_file = config_file
        self.cfg = configparser.ConfigParser(allow_no_value=True,
                                             delimiters="=")
        print(f"Parsing {self.config_file}")
        self.cfg.read(self.config_file)
        self.host = self.cfg['mysql']['host']
        self.user = self.cfg['mysql']['username']
        self.password = self.cfg['mysql']['password']
        self.charset = self.cfg['mysql']['charset']
        self.database = self.cfg['mysql']['database']
        self.db_check = self.cfg['mysql']['db_check_flag']

    def connect(self):
        print("Establishing MySQL Connection")
        if self.db_check == 1:
            print(f"Using database: {self.database}")
            self.con = pymysql.connect(host=self.host,
                                       user=self.user,
                                       password=self.password,
                                       charset=self.charset,
                                       database=self.database,
                                       autocommit=True,
                                       cursorclass=pymysql.cursors.DictCursor)
        else:
            print("No database check. Use select_db(database_name).")
            self.con = pymysql.connect(host=self.host,
                                       user=self.user,
                                       password=self.password,
                                       charset=self.charset,
                                       autocommit=True,
                                       cursorclass=pymysql.cursors.DictCursor)

    def write(self, stmt: str, vals: None | str | tuple) -> int:
        self.con.ping(reconnect=True)
        with self.con.cursor() as cursor:
            print(f"Writing to database: {stmt} + {vals}")
            if vals is None:
                return cursor.execute(stmt)
            else:
                return cursor.execute(stmt, vals)

    def read(self, stmt: str, vals: None | str | tuple, scope: str) -> tuple:
        """
        Returns 1 tuple for scope="ONE", tuple in tuple for scope="ALL"
        """
        self.con.ping(reconnect=True)
        with self.con.cursor() as cursor:
            print(f"Executing SQL: {stmt} + {vals}")
            cursor.execute(stmt, vals)
            if scope == "ONE":
                return cursor.fetchone()
            elif scope == "ALL":
                return cursor.fetchall()

    def create_db(self, database_name: str) -> int:
        self.con.ping(reconnect=True)
        with self.con.cursor() as cursor:
            print(f"Creating database {database_name}")
            return cursor.execute(f"CREATE DATABASE {database_name}")

    def select_db(self):
        self.con.ping(reconnect=True)
        if self.db_check == 0:
            with self.con.cursor() as cursor:
                print(f"Checking if database {self.database} exists")
                cursor.execute(f"SHOW DATABASES LIKE {self.database}")
                res = cursor.fetchone()
                if res:
                    self.con.select_db(self.database)
                else:
                    self.create_db(self.database)
                    self.con.select_db(self.database)
                    self.db_check = 1
        elif self.db_check == 1:
            self.con.select_db(self.database)

        with open(self.config_file, 'w') as cfgfile:
            print("Writing to config.ini")
            self.cfg.write(cfgfile)

    def get_info(self, command: str):
        self.con.ping(reconnect=True)
        if command == "HOST":
            return self.con.get_host_info()
        elif command == "PROTO":
            return self.con.get_proto_info()
        elif command == "SERVER":
            return self.con.get_server_info()
