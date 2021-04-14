import pandas 
import database_driver as db
import import_invoices as inv
import import_firms
import fdb

def select_status():
    con = db.connect_to_database()
    cur = con.cursor()

    query = "select STATUS from BLRC"
    cur.execute(query)

    for row in cur:
        print(row[0])


def update_status():
    """ Update status field of given invoice entry
        Temporary field insert test method.
    """
    con = db.connnect_to_database()
    cur = con.cursor()

    update = "update BLRC set STATUS=? where current of "
    cur.execute(update)


def update_invoice_status(index):
    """ Update status field of given invoice entry.
        Main method implementation.
        :param index: Index of given invoice in data frame
    """
    entry = inv.import_invoices(index)
    con = db.connect_to_database("prod")
    cur = con.cursor()

    query = "select STATUS, ZTDRUCKEN, ID from BLRC where LRECHNR=?"
    cur.execute(query, [entry['LRECHNR']])
    id = 0
    for row in cur:
        print(row)
        id = row[2]

    if entry['STATUS'] == 'Erledigt':
        update = "update BLRC set ZTDRUCKEN='J' where ID=?"
        cur.execute(update, [id])
        print("Updated progress to 'done'")

        update = "update BLRC set STATUS='B' where ID=?"
        cur.execute(update, [id])
        print("Updated status to 'booked'")

    if entry['STATUS'] == 'in Bearbeitung':
        update = "update BLRC set STATUS='D' where ID=?"
        cur.execute(update, [id])
        print("Updated done status to 'in progress'")

        query = "select STATUS, ZTDRUCKEN, ID from BLRC where LRECHNR=?"
        cur.execute(query, [entry['LRECHNR']])
        id = 0
        for row in cur:
            print(row)
            id = row[2]

    con.commit()


def switch_invoicer(old_firm, new_firm):
    """ Replace old invoice firm to a new firm name.
        :param old_firm: String name of old firm to replace.
        :param new_firm: String name of new firm to inject.
    """ 
    old_blief_id = import_firms.get_blief_id(import_firms.get_badr_id(old_firm))
    new_badr_id = import_firms.get_badr_id(new_firm)
    new_blief_id = import_firms.get_blief_id(new_badr_id)
    print(old_blief_id, new_blief_id)
    select = "select ID from BLRC where BLIEF_ID_LINKKEY=?"
    update = "update BLRC set BLIEF_ID_LINKKEY=?, BADR_ID_LADRCODE=? where ID=?"
    
    con = db.connect_to_database("prod")
    scroll = con.cursor()
    edit = con.cursor()
    scroll.execute(select, [old_blief_id])

    count = 0
    for row in scroll:
        edit.execute(update, [new_blief_id, new_badr_id, row[0]])
        count += 1

    con.commit()

    print("Altered {} rows.".format(count))


def update_badr_str():
    """ Driver method to update BADR table entries
    """
    con = db.connect_to_database('prod')
    cur = con.cursor()
    edit = con.cursor()
    select = "select NAME, ID from BADR where STR='0'"

    cur.execute(select)
    count = 0
    for row in cur:
        # edit.execute("update BADR set STR='', HAUSNR='' where ID=? returning ID", [row[1]])
        # print("Updated {} row {}".format(row[0], edit.fetchall()[0][0]))
        print(row)
        count += 1
    
    # con.commit()
    print(count)


def update_project_desc(index):
    """ Update project entities into invoice entries. 
        BAUVOR and LIEF.
    """
    con = db.connect_to_database('prod')
    cur = con.cursor()
    
    project = inv.import_invoice_openpxl(index)
    print(project)
    cur.execute("update BLRC set BAUVOR=?, LIEG=? where LRECHNR=?", [project['BAUVOR'], project['LIEG'], project['LRECHNR']])
    con.commit()
    

if __name__ == "__main__":
    print("edit_entries")
    update_project_desc(606)