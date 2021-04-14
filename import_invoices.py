import fdb
import pandas as pd
import re
import database_driver as db
import import_firms as firms
import datetime
import openpyxl
from shutil import copyfile


def import_invoices(index):
    """ Import excel file containing sample sales data
        File should be processed for specfic columns
        :param index: Index of dataframe row
    """
    sample_data = pd.read_excel(
        'master_invoice_data.xlsx', sheet_name='Rechnungen')
    # sample_data = sample_data.where(pandas.notnull(sample_data), None)
    # name = sample_data.to_dict()['Name']
    # brutto = sample_data.to_dict()['Brutto']
    invoice_data = {
        'NAME': sample_data.iloc[[index]]['Lieferant'].sum(),
        'RECHDATUM_LIEF': format_data(sample_data, index, 'Beleg Datum'),
        'RECHDATUM': format_data(sample_data, index, 'Beleg Datum'),
        'ZAHLDATUM': format_data(sample_data, index, 'Fälligkeit'),
        'LRECHNR': sample_data.iloc[[index]]['Rechnungs-Nr.'].sum(),
        'GESAMT': "{:.2f}".format(sample_data.iloc[[index]]['Rechnungsbetrag'].sum()),
        'STATUS': sample_data.iloc[[index]]['Status'].sum(),
        'ZAHL': format_data(sample_data, index, 'Bezahlt am'),
        'BAUVOR': sample_data.iloc[[index]]['Bauvorhaben'].values(),
        'LIEG': sample_data.iloc[[index]]['Liegenschaft'].values()
    }

    return invoice_data


def import_invoice_openpxl(index):
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


def ws_format(ws, index, col):
    """ Formatting for import_invoice_openpxl method
    """
    return ws['{}{}'.format(col, index + 1)].value


def format_data(sample_data, index, col):
    """ Convert to proper date string format for time series objects
        of excel dates
        :param sample_data: Dataframe file that is read
        :param index: Index of dataframe row 
        :param col: Column name of dataframe
    """
    try:
        return sample_data.iloc[[index]][col][index].strftime('%Y-%m-%d')
    except (AttributeError, ValueError):
        return datetime.datetime.now()


def insert_invoice(BLIEF_ID, BADR_ID, BMAND_ID, RECHDATUM_LIEF, RECHDATUM, ZAHLDATUM, LRECHNR, GESAMT):
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
    """ Process invoices of corresponding
        invoices into inserted to db
        :param index: Index of excel row number
    """
    try:
        invs = import_invoices(index)
    except AttributeError:
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


if __name__ == "__main__":
    """ Test runs import invoices
    """
    # counter = 0
    # for i in range(1360, len(db.excel_to_dataframe('master_invoice_data.xlsx', 'Rechnungen'))):
        # process_invoices(i)
        # print(import_invoices(i)['GESAMT'], i)

    # print(len(database_driver.excel_to_dataframe('master_invoice_data.xlsx', 'Rechnungen')))
    # 2242 Entries as of 24/03/21
    # print(counter)
    # print(import_invoices(5))
    # print(import_invoices(12))
    # process_invoices(7)
    pass