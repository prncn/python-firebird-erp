import fdb
import pandas as pd
import re
import database_driver as db
import import_firms as firms
import datetime
import openpyxl
from shutil import copyfile
from dateutil import parser
import edit_entries as edit

class invoice_data:
    def __init__(self, name, content):
        self.name = name
        self.content = content


STATUS_DICT = {
    'ERLEDIGT': 'E',
    'HZA': 'H',
    'IN BEARBEITUNG': 'B',
    'LASTSCHRIFT': 'L',
    'OFFEN': 'O',
    'OGV': 'V',
    'PRÜFEN': 'P',
    'ANWALT': 'A',
    'GERICHT': 'G',
    'INKASSO':'K',
    'RISK': 'R',
    'TECHFUND': 'T',
    'FA': 'F',
    'VERRECHNUNGSKONTO': 'X'
}


def load_entry_pandas(index: int) -> dict[str, pd.DataFrame]:
    """ Import excel file containing sample sales data
        File should be processed for specfic columns
        :param index: Index of dataframe row
    """
    sample_data = pd.read_excel(
        'OPS.xlsx', sheet_name='Rechnungen')
    # sample_data = sample_data.where(pandas.notnull(sample_data), None)
    # name = sample_data.to_dict()['Name']
    # brutto = sample_data.to_dict()['Brutto']
    def col(name):
        return sample_data.iloc[[index]][name][index]

    BPROJ_ID = ''
    if col('Bauvorhaben'):
        BPROJ_ID = col('Bauvorhaben')
    else:
        BPROJ_ID = col('Liegenschaften')

    ZTDRUCKEN = 'N'
    if col('Status') == 'Erledigt':
        ZTDRUCKEN = 'J'

    invoice_data = {
        'NAME': col('Rechnungssteller'),
        'RECHDATUM_LIEF': format_date(col('Beleg Datum')),
        'RECHDATUM': format_date(col('Beleg Datum')),
        'ZAHLDATUM': format_date(col('Fälligkeit')),
        'LRECHNR': col('Rechnungs-Nr.'),
        'ANPASSUNGDM': "{:.2f}".format(sample_data.iloc[[index]]['Brutto'].sum()),
        'STATUS': col('Status'),
        'ZAHLDATUM': filter_date(col('Bezahlt am'), col('Fälligkeit')),
        'BPROJ_ID': BPROJ_ID,
        'ZTDRUCKEN': ZTDRUCKEN

    }

    edit_invoice = {}
    for key, value in invoice_data.items():
        if not pd.isna(value) and value:
            edit_invoice[key] = value

    return edit_invoice


def filter_date(check_str, alt):
    """ Check first if date exists or whether it is a string,
        then pass to formatter
    """
    if check_str == "Bezahlt":
        return format_date(alt)

    res = True
    try:
        res = bool(datetime.datetime.strptime(str(check_str), '%d-%m-%Y'))
    except ValueError:
        res = False

    if res:
        return format_date(check_str)
    else:
        return None


def load_entry_openpyxl(index: str) -> dict[str, str]:
    """ Replacement of import_invoice method
        that uses openpxyl instead of pandas excel reader
    """
    wb = openpyxl.load_workbook('master_invoice_data.xlsx')
    ws = wb['Rechnungen']

    invoice_data = {
        'NAME': ws_format(ws, index, 'A'),
        'RECHDATUM_LIEF': ws_format(ws, index, 'J'),
        'RECHDATUM': ws_format(ws, index, 'J'),
        'ZAHLDATUM': ws_format(ws, index, 'K'),
        'LRECHNR': ws_format(ws, index, 'G'),
        'GESAMT': ws_format(ws, index, 'I'),
        'STATUS': ws_format(ws, index, 'N'),
        'ZAHL': ws_format(ws, index, 'O'),
        'BAUVOR': ws_format(ws, index, 'D'),
        'LIEG': ws_format(ws, index, 'E')
    }

    return invoice_data


def get_bprojpo(BPROJ):
    con, cur = db.init_db()

    cur.execute("select ID from BPROJPO where BPROJ_MASKENKEY = ?", [BPROJ])
    for row in cur:
        return row[0]

    cur.execute("insert into BPROJ (MASKENKEY) values (?)", [BPROJ])
    con.commit()
    cur.execute("insert into BPROJPO (BPROJ_MASKENKEY) values (?) returning ID", [BPROJ])
    con.commit()

    return cur.fetchall()[0][0]
    

def ws_format(ws: openpyxl.Workbook, index: int, col: int) -> str:
    """ Formatting for import_invoice_openpxl method
    """
    return ws['{}{}'.format(col, index + 1)].value


def format_date(date) -> datetime.datetime:
    """ Convert to proper date string format for time series objects
        of excel dates
        :param sample_data: Dataframe file that is read
        :param index: Index of dataframe row 
        :param col: Column name of dataframe
    """

    if(not isinstance(date, str)):
        date = datetime.datetime.now()
    
    if(not isinstance(date, datetime.datetime)):
        return ''

    return date.strftime('%d.%m.%Y')


def insert_invoice(BLIEF_ID, BADR_ID, BMAND_ID, RECHDATUM_LIEF, RECHDATUM, ZAHLDATUM, LRECHNR, GESAMT) -> int:
    """ Insert an entry of invoice into 
        main invoice table BLCR
        :param BLIEF_ID: Connected supplier table ID 
        :param BADR_ID: Connected address table ID
        :param BMAND_ID: Connected client table ID
        :param RECHDATUM_LIEF: Date of receipt
        :param RECHDATUM: Date of due payment
        :param ZAHLDATUM: Date of actual payment
        :param LRECHNR: Invoice number
        :param GESAMT: Total invoice amount
    """
    con = db.connect_to_database()
    insert = "insert into BLRC (BLIEF_ID_LINKKEY, BADR_ID_LADRCODE, BMAND_ID, RECHDATUM_LIEF, RECHDATUM, ZAHLDATUM, BWAER_ID_WAEHRUNGK, LRECHNR, ANPASSUNGDM) values (?, ?, ?, ?, ?, ?, 1, ?, ?) returning ID"

    cur = con.cursor()
    cur.execute(insert, [BLIEF_ID, BADR_ID, BMAND_ID,
                         RECHDATUM_LIEF, RECHDATUM, ZAHLDATUM, LRECHNR, GESAMT])
    blrc_id = cur.fetchall[0][0] 
    con.commit()
    con.close()

    return blrc_id


def process_invoices(index):
    """ Main driver. Profcess invoices of corresponding
        invoices into inserted to db
        :param index: Index of excel row number
    """
    try:
        invs = load_entry_pandas(index)
    except AttributeError as exp:
        print(exp.args + " on index: " + index)
        return

    BADR_ID = firms.get_badr_id(invs['NAME'])
    BLIEF_ID = firms.get_blief_id(BADR_ID)

    print(invs['GESAMT'])

    insert_invoice(
        BLIEF_ID,
        BADR_ID,
        1,
        invs['RECHDATUM_LIEF'],
        invs['RECHDATUM'],
        invs['ZAHLDATUM'],
        invs['LRECHNR'],
        invs['GESAMT'],
    )


def check_then_insert(invoice):
    """ Alternative insertion method. Only insert in case 
        entry does not exist. Reduce overhead by handling conditions through SQL directly.
    """
    con = db.connect_to_database('prod')
    cur = con.cursor()

    # Check if invoice has already been entered
    if 'LRECHNR' in invoice:
        cur.execute("select ID from BLRC where LRECHNR = ?", [invoice['LRECHNR']])
        for row in cur:
            print('Invoice already exists.')
            return None

    BADR_ID = firms.get_badr_id(invoice['NAME'])
    BLIEF_ID = firms.get_blief_id(BADR_ID)
    print(BADR_ID)
    
    BPROJ_ID = ''
    if 'BPROJ_ID' in invoice:
        BPROJ_ID = invoice['BPROJ_ID']

        bproj_exists = False
        cur.execute("select ID from BPROJ where BEZ = ?", [BPROJ_ID])
        for row in cur:
            bproj_exists = True
            BPROJ_ID = row[0]

        if not bproj_exists:
            print('Project entry does not exist. Inserting..')
            cur.execute('insert into BPROJ (BEZ, BSM_ID_VERANTW, BSM_ID_VERTRET, DATUM_VON, DATUM_BIS) values (?, 1, 1, CURRENT_DATE, CURRENT_DATE) returning ID', [BPROJ_ID])
            BPROJ_ID = cur.fetchall()[0][0]
            con.commit()

        print('BPROJ_ID: ' + str(BPROJ_ID))
        invoice['BPROJ_ID'] = BPROJ_ID


    fieldname_list = ', '.join(list(invoice.keys())[1:])
    fieldval_list = ', '.join(str(x) for x in list(invoice.values())[1:])
    prep_list = edit.prep_list(invoice)[3:]
    insert = 'insert into BLRC (BMWST_ID_MWSTKZ, BWAER_ID_WAEHRUNGK, BMAND_ID, BLIEF_ID_LINKKEY, BADR_ID_LADRCODE, {}) values (5, 1, 1, ?, ?, {}) returning ID'.format(fieldname_list, prep_list)

    exec_prep = [BLIEF_ID] + [BADR_ID] + list(invoice.values())[1:]
    print(exec_prep)
    print(insert)
    cur.execute(insert, exec_prep)

    con.commit()
    con.close()


if __name__ == "__main__":
    """ Test runs import invoices
    """
    # previous count 2053
    counter = 1174
    for i in range(counter, len(db.excel_to_dataframe('OPS.xlsx', 'Rechnungen'))):
        entry = load_entry_pandas(i)
        print(entry)
        check_then_insert(entry)
        print('COUNT' + str(i))

    # print(len(database_driver.excel_to_dataframe('OPS.xlsx', 'Rechnungen')))
    # 2242 Entries as of 24/03/21
    # print(counter)
    # print(import_invoices(5))
    # print(import_invoices(12))
    # process_invoices(7)

    # case = load_entry_pandas(1739)
    # print(case)

    # entry = load_entry_pandas(648)
    # print(entry)
    # check_then_insert(entry)

    pass
