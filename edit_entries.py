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


def update_invoice_status(index: int):
    """ Update status field of given invoice entry.
        Main method implementation.
        :param index: Index of given invoice in data frame
    """
    """ Update status field of given invoice entry.
        Main method implementation.
        :param index: Index of given invoice in data frame
    """
    entry = inv.load_entry_pandas(index)
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


def switch_invoicer(old_firm: str, new_firm: str):
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
        Driver meth
    """
    con = db.connect_to_database('prod')
    cur = con.cursor()
    edit = con.cursor()
    select = "select NAME, ID from BADR where STR='0'"

    cur.execute(select)
    count = 0
    for row in cur:
        edit.execute("update BADR set STR='', HAUSNR='' where ID=?", [row[1]])
        print("Updated {} row {}".format(row[0], row[1]))
        print(row)
        count += 1
    
    con.commit()
    print(count)


def update_badr_str(index):
    """ Driver method to update BADR table entries
    """
    con = db.connecto_to_database('prod')
    cur = con.cursor()

    project = inv.load_entry_pandas(index)
    cur.execute("update BLRC set BAUVOR==, LIEG== where LRECHNR=?")
    con.commit()


def update_project_desc(index: int):
    """ Update project entities intro invoice entries.
        BAUVOR and LIEF
    """
    con = db.connect_to_database('prod')
    cur = con.cursor()

    project = inv.load_entry_pandas(index)
    if project['BAUVOR'] and project['LIEG'] is None:
        return

    print(project)
    cur.execute("update BLRC set BAUVOR=?, LIEG=? where LRECHNR=?", [project['BAUVOR'], project['LIEG'], project['LRECHNR']])
    con.commit()


def markdone_all():
    """ Mark an invoice as finished. This will be used to
        update the complete collection of old invoices, to be 
        prepared for new data. 
    """ 
    con = db.connect_to_database('prod')
    cur = con.cursor()

    cur.execute("update BLRC set ZTDRUCKEN='J' where ZTDRUCKEN='N'")    
    con.commit()


def company_nuke():
    """ WARNING clearance, deletion of complete firm entry tables
        BLIEF then BADR
    """
    con, cur = db.init_db()
    cur.execute("delete from BANSP where ID > 0")
    con.commit()
    cur.execute("delete from BLIEF where ID > 0")
    con.commit()
    cur.execute("delete from BADR where ID > 2")
    con.commit()


def invoice_nuke():
    con, cur = db.init_db()
    cur.execute("delete from BLRC where ID > 0")
    con.commit()


def prep_list(dict):
    """ Return string for prepared statement 
        to be inserted in method
    """
    prep_list = []
    for x in list(dict.values()):
        prep_list.append('?')
    return ', '.join(prep_list)


if __name__ == "__main__":
    # print("edit_entries")
    # count = 167
    # for i in range(count, len(db.excel_to_dataframe('lieferanten_uebersicht.xlsx', 'Orginal').index)):  
    #     update_project_desc(i)
    #     # count += 1
    #     print(count) 
    # update_badr_str(55)
    invoice_nuke()

    pass

# EXECUTE PROCEDURE P_TABELLEN_EINTRAGEN;
# EXECUTE PROCEDURE P_VIEWS_EINTRAGEN;
# EXECUTE PROCEDURE P_CHECK_TABELLEN_FELDER(1);