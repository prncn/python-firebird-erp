import fdb
import pandas as pd
import re
import database_driver


def get_supplier_data(index):
    """ Import row of supplier data by given index
        :param: index - Index to be read from dataframe
        :return: Dict of supplier info
    """
    df = database_driver.excel_to_dataframe('lieferanten_uebersicht.xlsx', 'Orginal')
    field_data = {
        'NAME': df.iloc[[index]]['Supplier Name'].sum(),
        'ABTEILUNG': format_position(str(df.iloc[[index]]['Position'].sum())),
        'STR': df.iloc[[index]]['Street'].sum(),
        'HAUSNR': df.iloc[[index]]['Street No.'].sum(),
        'PLZ': df.iloc[[index]]['Postcode'].sum(),
        'EMAIL': df.iloc[[index]]['E-Mail'].sum(),
        'WEBSITE': df.iloc[[index]]['WEB'].sum(),
        'TEL1': df.iloc[[index]]['Telefon'].sum(),
        'TEL2': df.iloc[[index]]['Mobil'].sum(),
        'FAX': df.iloc[[index]]['Fax'].sum(),
        'ANSP': df.iloc[[index]]['Ansprechpartner'].sum(),
        'KNR': df.iloc[[index]]['Kundennummer'].sum()
    }

    for key, value in field_data.items():
        if not value and key != 'ANSP':
            field_data[key] = None

    return field_data


def key_count(entries, key):
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


def insert_badr(supplier):
    """ Insert a specified supplier into Firebird database.
        Table BADR is supplied with a key
        :param supplier: Supplier object to be inserted
        :retun: Return ID of address master list
    """
    con = database_driver.connect_to_database()
    insert_badr = "insert into BADR (NAME, ABTEILUNG, BPLZ_ID_LANDPLZ, WEBSITE, EMAIL, STR, HAUSNR, TELVOR, TELANSCH, TELVOR2, TELANSCH2, FAXVOR, FAXANSCH) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) returning ID"

    cur = con.cursor()
    BPLZ_ID_LANDPLZ = 0
    if supplier['PLZ'] != 0:
        cur.execute("select ID from BPLZ WHERE PLZ = {}".format(supplier['PLZ']))
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
    # try:
    insert_bansp = "insert into BANSP (BMAND_ID, BADR_ID_LINKKEY, NAME, NACHNAME, EMAIL) values (1, ?, ?, ?, ?)"
    cur.execute(insert_bansp, [badr_id, supplier['ANSP'], supplier['ANSP'], supplier['EMAIL']])
    # except fdb.fbcore.DatabaseError:
        # print(badr_id, supplier['ANSP'], supplier['EMAIL'])
    con.commit()
    con.close()

    return badr_id


def insert_badr_min(supplier):
    """ Insert minified entry of supplier into adresses table, a
        minified entry contains only a company name.
        This is used in case an invoice entry is detected with an
        unknown/new supplier name.

        :parasm supplier: Supplier name string to be inserted in to table 
    """
    con = database_driver.connect_to_database()
    cur = con.cursor()

    insert = "insert into BADR (NAME) values (?) returning ID"
    cur.execute(insert, [supplier])
    badr_id = cur.fetchall()[0][0]

    con.commit()
    con.close()

    return badr_id


def insert_blief(BADR_ID):
    """ Insert entry of supplier into joint table
        BLIEF of client addresses

        :param BADR_ID: Returned adress table entry ID
    """

    con = database_driver.connect_to_database()
    link_sup = "insert into BLIEF (BADR_ID_ADRNR, BWAER_ID_WAERUNGK, ERFDATUM, KZ_MWST, BBES_EINZELN) values (?, ?, CURRENT_DATE, 5, 1)"

    cur = con.cursor()
    cur.execute(link_sup, [BADR_ID, 1])
    con.commit()

    con.close()


def iterate_all_suppliers():
    """ Insert all entries of excel file data.
        Iterates full sheet and applies insertions
    """
    entries = database_driver.excel_to_dataframe()
    for entry in entries:
        gen_id = insert_badr(entry)
        insert_blief(gen_id)


def get_badr_id(name):
    """ Fetch the address id of the BADR table
        by a string name (company name)

        :param name: Name string of company
    """
    con = database_driver.connect_to_database()
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


def get_blief_id(BADR_ID):
    """ Fetch the supplier id of the BLIEF table
        by address id of BADR ID

        :param BADR_ID: Address table ID of connected entry
    """
    con = database_driver.connect_to_database()
    select = "select ID from BLIEF where BADR_ID_ADRNR = ?"

    cur = con.cursor()
    cur.execute(select, [BADR_ID])
    blief_id = cur.fetchall()[0][0]

    con.commit()
    con.close()

    return blief_id


def format_number(number):
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


def replace_zero(dict):
    """ Replace zero entries to None types.
        :param dict: Dictionary object containing zeroes
    """
    for key, value in dict.items():
        if value == 0 or value == '0':
            dict[key] = None


def format_employee_name(name):
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


def format_position(position):
    """ Format employee position strings correctly.
        :param position: Position description name
    """
    if (position == "0"):
        return None
    if position is not None:
        return re.split(',|/', position)[0]
    
    return None


if __name__ == "__main__":
    """ Test runs db scripts
    """
    # badr_id = insert_master("Mercedes-Benz")
    # insert_supplier(badr_id)
    # clear_entries()
    # insert_invoice()
    # get_badr_id("Mercedes-Benz")
    # process_invoices()

    for i in range (0, len(database_driver.excel_to_dataframe('lieferanten_uebersicht.xlsx', 'Orginal').index)):    
        entr = get_supplier_data(i)
        # badr_id_entr = insert_badr(entr) 
        # insert_blief(badr_id_entr)

    # for i in range (10, 15):
        # entr = get_supplier_data(i)
        # badr_id_entr = insert_badr(entr)

    # insert_blief(insert_badr(get_supplier_data(10)))
    # insert_blief(insert_badr(get_supplier_data(11)))
    # clear_entries()

    # entr = get_supplier_data(5)
    # print(entr)