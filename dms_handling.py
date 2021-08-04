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
    try:
        blrc_id = cur.fetchall()[0][0]
    except IndexError:
        print("Index error occured when fetching invoice id")
        return None

    link = inv_no.hyperlink
    if link is not None:
        print(link.target)
        print(blrc_id)
        return {
            'id': blrc_id,
            'link': r'\\192.168.178.245\dms' + link.target[2:].replace('%20', ' ')
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

    if doc is None:
        return

    src = doc['link']
    filename = ntpath.basename(doc['link'])
    blrc_id = doc['id']

    base_url = r'\\192.168.178.51\AvERP_DB\DMS_IMPORT\BLRC\ID{} {}'.format(blrc_id, filename)

    try:
        copyfile(src, base_url)
    except FileNotFoundError:
        print("Path not found. Trying alt path... (REPLACE CHARS)")
        try:
            copyfile(src, base_url.replace("& ", ""))
        except FileNotFoundError:
            try:
                print("Path not found. Trying alt path... (NESTED FOLDER))")
                filename_index = src.rfind('\\')
                folder_name = src[filename_index + 1 : src.find('_RE', filename_index)]
                new_src = src[:filename_index] + '\\' + folder_name + src[filename_index:]
                print(new_src)
                copyfile(new_src, r'\\192.168.178.51\AvERP_DB\DMS_IMPORT\BLRC\ID{} {}'.format(blrc_id, filename))
            except FileNotFoundError:
                print("Path not found. Trying alt path... (SPLIT FOLDER NAMES)")
                folder_name = folder_name.split(' ')[0]
                new_src = src[:filename_index] + '\\' + folder_name + src[filename_index:]
                print(new_src)
                copyfile(new_src, r'\\192.168.178.51\AvERP_DB\DMS_IMPORT\BLRC\ID {}'.format(blrc_id, filename))


if __name__ == "__main__":
    # path = fetch_hyperlink(3)['link']
    # print(path)

    count = 1840
    for i in range(count, len(db.excel_to_dataframe('master_invoice_data.xlsx', 'Rechnungen').index)):  
        copy_document(i)
        print('count: ' + str(count))
        count += 1

    