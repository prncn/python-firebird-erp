import fdb
import pandas as pd
import re
from timeit import default_timer as timer
import import_invoices as invoices


def connect_to_database(status: str='prod') -> fdb.Connection:
    """ Connect to the firebird database
        Default database to connect is the AVERP empty db
        :return: Connection object
    """
    # fb_library_name=r"C:\Users\Princen.Vijayakumar\Downloads\Firebird-2.5.9.27139-0_x64\bin\fbclient.dll"
    if status == 'prod':
        print("Altering production database...")
        con = fdb.connect(
            host='192.168.178.51', database='/AvERPDB/XDIRECT_DB.FDB', user='SYSDBA',
            password='masterkey', charset='UTF8'
        )
    else:
        con = fdb.connect(
            dsn='C:/Program Files (x86)/AVERP/AVERP.FDB', user='SYSDBA',
            password='masterkey', charset='UTF8'
        )

    return con


def excel_to_dataframe(file_name: str, sheet_name: str) -> pd.DataFrame:
    """ Load in excel file of supplier list data
        This should be injected into Firebird / Averp supplier info
        :param file_name: File name of excel file to be read
        :param sheet_name: Excel sheet data to be read
        :return: Dataframe of read excel file
    """
    supplier_data = pd.read_excel(file_name, sheet_name)
    supplier_data = supplier_data.where(pd.notnull(supplier_data), None)

    return supplier_data


def clear_entries() -> fdb.Connection:
    """ Clear all entries of created insertions
        of tables BADR and BLIEF
    """
    con = connect_to_database()
    delete = "delete from BADR where ID > 2"

    cur = con.cursor()

    try:
        cur.execute(delete)
    except fdb.fbcore.DatabaseError:
        cur.execute("delete from BANSP where BADR_ID_LINKKEY > 2")
        cur.execute("delete from BLIEF where BADR_ID_ADRNR > 2")
        cur.execute(delete)

    con.commit()
    con.close()

    return con


def performance_test():
    """ Performance test to compare to similar methods.
        Used to determine whether OpenPyXL or Pandas is appropriate.
    """
    times = []
    for i in range(0, 3):
        start = timer()
        for j in range(15, 18):
            # replaceable with any method
            # invoices.load_entry_openpyxl(j)   
            invoices.load_entry_pandas(j)
        end = timer()
        time = end - start
        times.append(time)

    total = 0
    for time in times:
        total += time

    print("Elapsed time of run: " + "{:.2f}".format(total/len(times)) + "s")


if __name__ == "__main__":
    performance_test()