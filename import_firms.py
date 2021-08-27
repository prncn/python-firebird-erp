import fdb
import pandas as pd
import re
import database_driver as db
import math
import numbers


def get_supplier_data(index: int) -> dict[str, pd.DataFrame]:
    """ Import row of supplier data by given index

        :param: index - Index to be read from dataframe
        :return: Dict of supplier info
    """
    df = db.excel_to_dataframe('lieferanten_uebersicht.xlsx', 'Orginal')
    col = df.iloc[index]
    field_data = {
        'NAME': col['Supplier Name'],
        'ABTEILUNG': format_position(str(df.iloc[[index]]['Position'].sum())),
        'STR': col['Street'],
        'HAUSNR': col['Street No.'],
        'PLZ': col['Postcode'],
        'EMAIL': col['E-Mail'],
        'WEBSITE': col['WEB'],
        'TEL1': col['Telefon'],
        'TEL2': col['Mobil'],
        'FAX': col['Fax'],
        'ANSP': col['Ansprechpartner'],
        'KNR': col['Kundennummer']
    }

    for key, value in field_data.items():
        if not value and key != 'ANSP':
            field_data[key] = None

    return field_data


def key_count(entries: pd.DataFrame, key: str) -> int:
    """ Returns the count of a specific key within the XLSX

        :param entries: Data of worksheet rows
        :param key: Key to searched for repetitions
        :return: Number of keys repitions
    """
    count = 0
    for entry in entries.items():
        if(key == entry[1]):
            count += 1

    return count


def insert_badr(supplier: dict) -> int:
    """ Insert a specified supplier into Firebird database.
        Table BADR is supplied with a key

        :param supplier: Supplier object to be inserted
        :retun: Return ID of address master list
    """
    con = db.connect_to_database()
    insert_badr = "insert into BADR (NAME, ABTEILUNG, BPLZ_ID_LANDPLZ, WEBSITE, EMAIL, STR, HAUSNR, TELVOR, TELANSCH, TELVOR2, TELANSCH2, FAXVOR, FAXANSCH) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) returning ID"

    cur = con.cursor()
    BPLZ_ID_LANDPLZ = ''
    if not math.isnan(supplier['PLZ']):
        print('FIRED HERE' + str(supplier['PLZ']))
        cur.execute(
            "select ID from BPLZ WHERE PLZ = {}".format(supplier['PLZ']))
        for id in cur:
            BPLZ_ID_LANDPLZ = id[0]
            break

    TEL1 = format_number(supplier['TEL1'])
    TEL2 = format_number(supplier['TEL2'])
    FAX = format_number(supplier['FAX'])
    print(TEL1, TEL2, FAX)
    cur.execute(insert_badr, [
        supplier['NAME'],
        supplier['ABTEILUNG'],
        BPLZ_ID_LANDPLZ,
        supplier['WEBSITE'],
        supplier['EMAIL'],
        supplier['STR'],
        supplier['HAUSNR'],
        TEL1['VOR'],
        TEL1['ANSCH'],
        TEL2['VOR'],
        TEL2['ANSCH'],
        FAX['VOR'],
        FAX['ANSCH']
    ])
    badr_id = cur.fetchall()[0][0]
    print(supplier['NAME'] + " inserted into BADR")
    try:
        insert_bansp = "insert into BANSP (BMAND_ID, BADR_ID_LINKKEY, NAME, NACHNAME, EMAIL) values (1, ?, ?, ?, ?)"
        cur.execute(insert_bansp, [badr_id, supplier['ANSP'],
                               supplier['ANSP'], supplier['EMAIL']])
    except fdb.fbcore.DatabaseError:
        print(badr_id, supplier['ANSP'], supplier['EMAIL'])
        pass
    con.commit()
    con.close()

    return badr_id


def insert_badr_min(supplier: dict) -> int:
    """ Insert minified entry of supplier into adresses table, a
        minified entry contains only a company name.
        This is used in case an invoice entry is detected with an
        unknown/new supplier name.

        :param supplier: Supplier name string to be inserted in to table 
        :return: ID of BADR entry 
    """
    con = db.connect_to_database()
    cur = con.cursor()

    insert = "insert into BADR (NAME) values (?) returning ID"
    cur.execute(insert, [supplier])
    badr_id = cur.fetchall()[0][0]

    con.commit()
    con.close()

    return badr_id


def insert_blief(BADR_ID: int) -> None:
    """ Insert entry of supplier into joint table
        BLIEF of client addresses

        :param BADR_ID: Returned adress table entry ID
    """
    con = db.connect_to_database()
    link_sup = "insert into BLIEF (BADR_ID_ADRNR, BWAER_ID_WAERUNGK, ERFDATUM, KZ_MWST, BBES_EINZELN) values (?, ?, CURRENT_DATE, 5, 1)"

    cur = con.cursor()
    cur.execute(link_sup, [BADR_ID, 1])
    con.commit()

    con.close()


def iterate_all_suppliers() -> None:
    """ Insert all entries of excel file data.
        Iterates full sheet and applies insertions
    """
    entries = db.excel_to_dataframe()
    for entry in entries:
        gen_id = insert_badr(entry)
        insert_blief(gen_id)


def get_badr_id(name: str) -> int:
    """ Fetch the address id of the BADR table
        by a string name (company name)

        :param name: Name string of company
    """
    con = db.connect_to_database('prod')
    select = "select ID from BADR where NAME = ?"

    cur = con.cursor()
    cur.execute(select, [name])
    try:
        badr_id = cur.fetchall()[0][0]
    except IndexError:
        print(name + " not found. Inserting...")
        badr_id = insert_badr_min(name)
        insert_blief(badr_id)

    con.commit()
    con.close()

    return badr_id


def get_blief_id(BADR_ID: int) -> int:
    """ Fetch the supplier id of the BLIEF table
        by address id of BADR ID

        :param BADR_ID: Address table ID of connected entry
    """
    con = db.connect_to_database('prod')
    select = "select ID from BLIEF where BADR_ID_ADRNR = ?"

    cur = con.cursor()
    cur.execute(select, [BADR_ID])
    blief_id = cur.fetchall()[0][0]

    con.commit()
    con.close()

    return blief_id


def format_number(number: int) -> dict:
    """ Method to format tel and fax numbers
        to database standard
        :param number: Unformatted number
    """
    VOR = ''
    ANSCH = number
    number = str(number)
    if number != 0:
        m = re.search(r'\D[^.]', number)
        if m:
            VOR = number[:m.start()]
            ANSCH = re.sub(r'\D', "", number[m.start():])

    formatted = {
        'VOR': VOR,
        'ANSCH': ANSCH
    }

    replace_zero(formatted)

    return formatted


def replace_zero(dict: dict) -> None:
    """ Replace zero entries to None types.
        :param dict: Dictionary object containing zeroes
    """
    for key, value in dict.items():
        if value == 0 or value == '0':
            dict[key] = None


def format_employee_name(name: str) -> str:
    """ Format position names correctly.
        This is to avoid string truncation, as firebird has restricted position name lengths.
        :param name: Unformatted name of employee
    """
    firstname = ''
    lastname = ''

    lastname = re.split(" ", name)[1]
    formatted = {
        'firstname': firstname,
        'lastname': lastname
    }

    replace_zero(formatted)

    return formatted


def format_position(position: str) -> None:
    """ Format employee position strings correctly.
        :param position: Position description name
    """
    if (position == "0"):
        return None
    if position is not None:
        return re.split(',|/', position)[0]

    return None


def delete_firm(name: str) -> None:
    """ Delete specific company by name key
        :param name: Name of company
    """
    con = db.connect_to_database()
    cur = con.cursor()

    delete = "delete from BADR where NAME = ? returning ID"
    cur.execute(delete, [name])
    id_del = cur.fetchall()[0][0]
    print("Deleted company entry {} with ID {}".format(name, id_del))
    con.commit()
    con.close()


def read_datev(index) -> dict:
    cols = ['Konto', 'Beschriftung', 'Unternehmensgegenstand',
            'Kunden-Nr.', 'Postfach oder Stra�e']
    df = pd.read_csv('datev_firms.csv',
                     skip_blank_lines=False,
                     header=9,
                     sep=';',
                     usecols=cols
                     )

    # df.dropna(inplace=True, how='all')
    index_corrected = index * 4
    col = df.iloc[index_corrected]
    col_next = df.iloc[index_corrected + 1]
    field_data = {
        'NAME': col['Beschriftung'],
        'STR': col['Postfach oder Stra�e'],
        'PLZ': (col_next['Postfach oder Stra�e']),
        'TEL1': col_next['Unternehmensgegenstand'],
        'KNR2': cast_int(col['Konto']),
        'KNR': col['Kunden-Nr.']
    }

    return field_data


def take_numeric(str):
    if str is not None:
        # key = re.search(r"\d+", str)
        # if key is not None:
        #     return int(key.group(0))

        return ''.join(char for char in str if char.isnumeric())


def cast_int(num) -> int:
    if math.isnan(num):
        return None
    return int(num)


def check_datev(index) -> bool:
    name = read_datev(index)['NAME']
    newname = read_datev(index)['PLZ']

    con = db.connect_to_database()
    cur = con.cursor()
    select = "select first 1 ID from BADR where NAME = ?"
    cur.execute(select, [name])

    for id in cur:
        return False

    # badr_id_entr = insert_badr(name)
    # insert_blief(badr_id_entr)
    return True


if __name__ == "__main__":
    """ Test runs db scripts
    """
    # badr_id = insert_master("Mercedes-Benz")
    # insert_supplier(badr_id)
    # clear_entries()
    # insert_invoice()
    # get_badr_id("Mercedes-Benz")
    # process_invoices()

    count = 51
    for i in range (count, len(db.excel_to_dataframe('lieferanten_uebersicht.xlsx', 'Orginal').index)):
        print(i)
        entr = get_supplier_data(i)
        print(entr)
        badr_id_entr = insert_badr(entr)
        insert_blief(badr_id_entr)

    # for i in range (10, 15):
    # entr = get_supplier_data(i)
    # badr_id_entr = insert_badr(entr)

    # insert_blief(insert_badr(get_supplier_data(10)))
    # insert_blief(insert_badr(get_supplier_data(11)))
    # clear_entries()

    # entr = get_supplier_data(5)
    # print(entr)

    # for i in range(2, 200):
    #     # if not check_datev(i):
    #     #     print(i, read_datev(i))
    #     #     print('DATA IS NEW')
    #     #     break
    #     try:
    #         check_datev(i)
    #     except:
    #         print(i)


