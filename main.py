import mysql.connector as conn
from getpass import getpass
from prettytable import PrettyTable

class DB:

    db = None
    cursor = None

    @classmethod
    def initialize_db(cls, username, password):    
        cls.db = conn.connect(
            user=username,
            password=password
        )

        cls.cursor = cls.db.cursor()

        # Check if 'testdb' database exist
        cls.cursor.execute("show databases")

        if "testdb" not in [i[0] for i in cls.cursor.fetchall()]:
            cls.cursor.execute("create database testdb")

        cls.cursor.execute("use testdb")

        # check if 'testtable' table exist
        cls.cursor.execute("show tables")
        
        if "testtable" not in [i[0] for i in cls.cursor.fetchall()]:
            # cls.cursor.execute("drop table testtable")

            cls.cursor.execute("""create table testtable (
                    rrn varchar(12) PRIMARY KEY NOT NULL,
                    name varchar(30) NOT NULL,
                    department varchar(6) NOT NULL,
                    fees float NOT NULL,
                    fees_paid tinyint(1) NOT NULL,
                    
                    CONSTRAINT rrn_check CHECK (char_length(rrn) = 12),
                    CONSTRAINT name_check CHECK (char_length(name) <= 30),
                    CONSTRAINT department_check CHECK (department in ('b.tech', 'm.tech', 'bsc', 'bca', 'bba')),
                    CONSTRAINT fees_check CHECK (fees >= 0),
                    CONSTRAINT fees_paid_check CHECK (fees_paid in (0, 1))
            )""")
        cls.db.commit()

    @classmethod
    def commit_db(cls):
        cls.db.commit()

    @classmethod
    def close_db(cls):
        cls.db.close()


def prompt_field(prompt_text):
    fields = ['rrn', 'name', 'department', 'fees', 'fees_paid']
    field = None

    while True:

        print(f"Fields are {fields}")
        field = input(prompt_text)
        if field.lower().strip() not in fields:
            print("\nInvalid field\n")
            continue

        break

    return field

def prompt_operator(prompt_text):
    operators = ['=', '<', '<=', '>', '>=', '<>']
    operator = None

    while True:

        print(f"Operators are {operators}")
        operator = input(prompt_text)
        if operator.strip() not in operators:
            print("\nInvalid operator\n")
            continue

        break

    return operator

while True:

    username = input("Enter mysql username: ")
    password = getpass("Enter mysql password: ")

    try:
        DB.initialize_db(username, password)
    except Exception as a:
        print(a)
        print("Invalid credentials")
        continue
    else:
        break

while True:

    print("\n----------------- Mysql Database Interface -----------------")
    print("1. Describe table")
    print("2. Insert row")
    print("3. Update row(s)")
    print("4. Delete row(s)")
    print("5. View table")
    print("6. Exit")

    choice = input("Enter choice: ").strip()

    if choice == "1":

        DB.cursor.execute("describe testtable")

        table = PrettyTable()
        table.field_names = ["Field", "Type", "Null", "Key", "Default", "Extra"]

        table.add_rows([[x.decode() if type(x) == type(b'') else str(x) for x in i] for i in DB.cursor.fetchall()])
        print(table)
    
    elif choice == "2":

        while True:

            print("rrn_check<constraint>: length of rrn should be 12")
            rrn = input("Enter RRN: ")

            print("name_check<constraint>: length of name should be less than 30")
            name = input("Enter Name: ")

            print("department_check<constraint>: department should be one of ('b.tech', 'm.tech', 'bsc', 'bca', 'bba'")
            dept = input("Enter Department: ").lower()

            print("fees_check<constraint>: Fees should be greater than or equal to 0")
            fees = input("Enter Fees: ")

            print("fees_paid_check<constraint>: fees_paid should be one of (0, 1)")
            fees_paid = input("Enter Fees_Paid: ")

            try:
                DB.cursor.execute(f"SET @rrn = '{rrn}'")
                DB.cursor.execute(f"SET @name = '{name}'")
                DB.cursor.execute(f"SET @dept = '{dept}'")
                DB.cursor.execute(f"SET @fees = '{fees}'")
                DB.cursor.execute(f"SET @fees_paid = '{fees_paid}'")
                DB.cursor.execute("insert into testtable values(@rrn,@name,@dept,@fees,@fees_paid)")
            except Exception as e:
                print("\nInsertion Failed\n")
                print(e, "\n")
            else:
                DB.commit_db()
                print("\nInsertion successful\n")
                break
    
    elif choice == "3":

        while True:

            set_field = prompt_field("Enter field to update: ")
            set_value = input("Enter the new value: ")

            where_field = prompt_field("Enter field to include in WHERE clause: ")
            where_value = input("Enter value to include in WHERE clause: ")

            operators = ['=', '<', '<=', '>', '>=', '<>']

            print(f"Operators are {operators}")
            operator = input("Enter operator to use in WHERE clause: ")
            if operator.strip() not in operators:
                print("\nInvalid operator\n")
                continue

            try:
                DB.cursor.execute(f"SET @setvalue = '{set_value}'")
                DB.cursor.execute(f"SET @wherevalue = '{where_value}'")
                DB.cursor.execute(f"update testtable set {set_field}=@setvalue where {where_field}{operator}@wherevalue")

            except Exception as e:
                print("\nUpdation Failed\n")
                print(e, "\n")
            else:
                DB.commit_db()
                print("\nUpdation successful\n")
                break

    elif choice == "4":

        while True:

            fields = ['rrn', 'name', 'department', 'fees', 'fees_paid']

            field = prompt_field("Enter field to include in WHERE clause: ")
            value = input("Enter value to include in WHERE clause: ")

            operator = prompt_operator("Enter operator to use in WHERE clause: ")
            
            try:
                DB.cursor.execute(f"SET @value = '{value}'")
                DB.cursor.execute(f"delete from testtable where {field}{operator}@value")

            except Exception as e:
                print("\nDeletion Failed\n")
                print(e, "\n")
            else:
                DB.commit_db()
                print("\nDeletion successful\n")
                break
    
    elif choice == "5":

        fields = ['rrn', 'name', 'department', 'fees', 'fees_paid']

        while True:

            print(f"Fields are {fields}")
            selected_fields = input("Enter fields to view (comma separated) (leave empty for all): ")

            query_fields = ""
            table = PrettyTable()

            if selected_fields.strip() == "":
                
                table.field_names = ["RRN", "NAME", "DEPARTMENT", "FEES", "FEES_PAID"]
                query_fields = "*"

            else:

                invalid = 0
                all_fields = []
                for i in selected_fields.split(","):
                    if i.lower().strip() not in fields:
                        print("\nInvalid field format\n")
                        invalid = 1
                        break
                    else:
                        all_fields.append(i.lower().strip())
                
                if invalid == 1: continue

                if len(list(all_fields)) != len(set(all_fields)):
                    print("\nField values must not repeat\n")
                    continue

                table.field_names = [i.upper().strip() for i in all_fields]

                for i in table.field_names:
                    query_fields += f"{i}, "

                query_fields = query_fields[0:-2]

            select_query = ""

            while True:

                where_choice = input("Enter 1 to add WHERE clause, 2 not to add: ").strip()

                if where_choice == "1":

                    where_field = prompt_field("Enter field to include in WHERE clause: ")
                    where_value = input("Enter value to include in WHERE clause: ")

                    operator = prompt_operator("Enter operator to use in WHERE clause: ")
                    
                    DB.cursor.execute(f"SET @value = '{where_value}'")
                    select_query = f"select {query_fields} from testtable where {where_field}{operator}@value"

                    break

                elif where_choice == "2":
                    select_query = f"select {query_fields} from testtable"
                    break

                else:

                    print("\nInvalid Choice\n")
                    continue

            DB.cursor.execute(select_query)

            table.add_rows(DB.cursor.fetchall())
            print(table)

            break

    elif choice == "6":
        DB.close_db()
        print("Thank you..")
        print("Exiting..")
        break

    else:
        print("Invalid choice")