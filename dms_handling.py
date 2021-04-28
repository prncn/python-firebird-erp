import database_driver as db
import import_invoices as invoices
import openpyxl
from shutil import copyfile
import ntpath


def fetch_hyperlink(index: int) -> None:
    """ Fetch hyperlink pointing to a specific document
        file location for DMS by a given excel data row
        :param index: Index of excel sheet to extract link from.
        :return: Dict containg hyperlink and its BLRC id
    """
    wb = openpyxl.load_workbook('master_invoice_data.xlsx')
    ws = wb['Rechnungen']

    inv_no = ws['G{}'.format(index + 1)]

    con = db.connect_to_database('prod')
    select = "select ID from BLRC where LRECHNR = ?"
    cur = con.cursor()
    cur.execute(select, [inv_no.value])
    blrc_id = cur.fetchall()[0][0]

    link = inv_no.hyperlink
    if link is not None:
        print(link.target)
        print(blrc_id)
        return {
            'id': blrc_id,
            'link': link.target
        }
    else:
        print("No document on record.")

    return None


def copy_document(index: int) -> None:
    """ Method to copy file from source path to 
        designated DMS import file path (from link). 
        AVERP then handles import into the database.
        :param index: Index of databframe of hyperlink to be fetched
    """
    doc = fetch_hyperlink(index)
    src = doc['link']
    filename = ntpath.basename(doc['link'])
    blrc_id = doc['id']
    copyfile(src, '/AVERPDB/DMS_IMPORT/BLRC/ID{} {}.pdf'.format(blrc_id, filename))


if __name__ == "__main__":
    pass