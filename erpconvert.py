import fdb
import pandas
import re


def excel_to_dataframe(file_name, sheet_name):
    """ Load in excel file of supplier list data
        This should be injected into Firebird / Averp supplier info
        :return: DataFrame of read excel file
    """
    supplier_data = pandas.read_excel(file_name, sheet_name)
    supplier_data = supplier_data.where(pandas.notnull(supplier_data), None)

    return supplier_data


def get_supplier_data(index):
    """ Import row of supplier data by given index
        :param: Index to be read from dataframe
        :return: Dict of supplier info
    """
    df = excel_to_dataframe('Lieferanten_Übersicht.xlsx', 'Orginal')
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
        'ANSP': df.iloc[[index]]['Ansprechpartner'].sum()
    }

    print(field_data['ABTEILUNG'])

    return field_data


def import_invoices(index):
    """ Import excel file containing sample sales data
        File should be processed for specfic columns
    """
    sample_data = pandas.read_excel(
        'sample_data_clear.xlsx', sheet_name='Rechnungen')
    # sample_data = sample_data.where(pandas.notnull(sample_data), None)
    # name = sample_data.to_dict()['Name']
    # brutto = sample_data.to_dict()['Brutto']
    name = sample_data.iloc[[index]]['Name'].sum()
    beleg_datum = sample_data.iloc[[index]]['Beleg Datum'].sum()
    eing_datum = sample_data.iloc[[index]]['Eingangs Datum'].sum()
    faellig_datum = sample_data.iloc[[index]]['Fälligkeit'].sum()
    rech_nr = sample_data.iloc[[index]]['Rechnungs-Nr.'].sum()
    brutto = sample_data.iloc[[index]]['Brutto'].sum()

    return [name, beleg_datum, eing_datum, faellig_datum, rech_nr, brutto]


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


def connect_to_database():
    """ Connect to the firebird database
        Default database to connect is the AVERP empty db
        :return: Connection object
    """
    con = fdb.connect(
        dsn='C:\Program Files (x86)\AVERP\AVERP.FDB', user='SYSDBA',
        password='masterkey', charset='UTF8'
    )

    return con


def insert_badr(supplier):
    """ Insert a specified supplier into Firebird database.
        Table BADR is supplied with a key
        :param supplier: Supplier object to be inserted
        :retun: Return ID of address master list
    """
    con = connect_to_database()
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
    WEB = supplier['WEBSITE'] if supplier['WEBSITE'] else None
    print(TEL1, TEL2, FAX)
    cur.execute(insert_badr, [
        supplier['NAME'],
        supplier['ABTEILUNG'],
        BPLZ_ID_LANDPLZ,
        WEB, 
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


def insert_blief(BADR_ID):
    """ Insert entry of supplier into joint table
        BLIEF of client addresses
    """

    con = connect_to_database()
    link_sup = "insert into BLIEF (BADR_ID_ADRNR, BWAER_ID_WAERUNGK, ERFDATUM, KZ_MWST, BBES_EINZELN) values (?, ?, CURRENT_DATE, 5, 1)"

    cur = con.cursor()
    cur.execute(link_sup, [BADR_ID, 1])
    con.commit()

    con.close()


def iterate_all_suppliers():
    """ Insert all entries of excel file data.
        Iterates full sheet and applies insertions
    """
    entries = excel_to_dataframe()
    for entry in entries:
        gen_id = insert_badr(entry)
        insert_blief(gen_id)


def clear_entries():
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


def insert_invoice(BLIEF_ID, BADR_ID, BMAND_ID, RECHDATUM_LIEF, RECHDATUM, ZAHLDATUM, LRECHNR, GESAMT):
    """ Insert an entry of invoice into 
        main invoice table BLCR
    """
    con = connect_to_database()
    insert = "insert into BLRC (BLIEF_ID_LINKKEY, BADR_ID_LADRCODE, BMAND_ID, RECHDATUM_LIEF, RECHDATUM, ZAHLDATUM, BWAER_ID_WAEHRUNGK, LRECHNR, ANPASSUNGDM) values (?, ?, ?, ?, ?, ?, 1, ?, ?)"

    cur = con.cursor()
    cur.execute(insert, [BLIEF_ID, BADR_ID, BMAND_ID,
                         RECHDATUM_LIEF, RECHDATUM, ZAHLDATUM, LRECHNR, GESAMT])
    con.commit()
    con.close()


def process_invoices(index):
    """ Process invoices of corresponding
        invoices to to inserted to db
    """
    init_data = import_invoices(index)

    BADR_ID = get_badr_id(init_data[0])
    BLIEF_ID = get_blief_id(BADR_ID)
    RECHDATUM_LIEF = init_data[1]
    RECHDATUM = init_data[2]
    ZAHLDATUM = init_data[3]
    LRECHNR = init_data[4]
    GESAMT = init_data[5]

    print(GESAMT)

    insert_invoice(BLIEF_ID, BADR_ID, 1, RECHDATUM_LIEF, RECHDATUM, ZAHLDATUM, LRECHNR, GESAMT)


def get_badr_id(name):
    """ Fetch the address id of the BADR table
        by a string name (company name)
    """
    con = connect_to_database()
    select = "select ID from BADR where NAME = ?"

    cur = con.cursor()
    cur.execute(select, [name])
    badr_id = cur.fetchall()[0][0]

    con.commit()
    con.close()

    return badr_id


def get_blief_id(BADR_ID):
    """ Fetch the supplier id of the BLIEF table
        by address id of BADR ID
    """
    con = connect_to_database()
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
    """
    for key, value in dict.items():
        if value == 0 or value == '0':
            dict[key] = None


def format_employee_name(name):
    """ Format position names correctly.
        This is to avoid string truncation, as firebird has restricted position name lengths.
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
    """
    if (position == "0"):
        return None
    if position is not None:
        return re.split(',|/', position)[0]
    
    return None


if __name__ == "__main__":
    """ Test runs db runs
    """
    # badr_id = insert_master("Mercedes-Benz")
    # insert_supplier(badr_id)
    # clear_entries()
    # insert_invoice()
    # get_badr_id("Mercedes-Benz")
    # process_invoices()

    for i in range (0, len(excel_to_dataframe('Lieferanten_Übersicht.xlsx', 'Orginal').index)):    
        entr = get_supplier_data(i)
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