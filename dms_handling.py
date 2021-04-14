import database_driver as db
import import_invoices as invoices
import openpyxl
from shutil import copyfile
import ntpath


def fetch_hyperlink(index):
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
    if link != None:
        print(link.target)
        print(blrc_id)
        return {
            'id': blrc_id,
            'link': link.target
        }

    return None


def copy_document(index):
    """ Move (copy) document from source path to ERP specified
        DMS location for import
        :param src: Original source path
    """
    doc = fetch_hyperlink(index)
    src = doc['link']
    filename = ntpath.basename(doc['link'])
    blrc_id = doc['id']
    copyfile(src, '/AvERPDB/DMS_IMPORT/BLRC/ID{} {}.pdf'.format(blrc_id, filename))


if __name__ == "__main__":
    pass