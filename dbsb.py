import sqlite3
import pickle
import hashlib
import CalcFarm_Database_Analyser_2 as db

def md5sum(t):
    return hashlib.md5(t).hexdigest()

def type_load(t):
    """
    :param encoded_value: This value was loaded directly from the database
    :return:
    """
    return [1] #pickle.loads(t)




sqlite3.enable_callback_tracebacks(True)
#con.create_function("load", 1, type_load)

#cur.execute("pragma table_info(testing);")
#print(cur.fetchall())
#b = pickle.dumps([1,2,3,4])
#print(type_load(b))

#cur.execute("select load(?)", (b,))
#print(cur.fetchone()[0])
#con.close()




con = sqlite3.connect("hi")
code = """create table testing2
(
	id int
		constraint testing_pk
			primary key,
	value longblob not null
);"""

#con.create_table(code)

#con.dump_data("testing", {"id":1, "value":[1,2,3]})
with con:
    cur = con.cursor()
    cur.execute(code)
    cur.execute("insert into testing2 (id, value) values (2, 3);")
    cur.execute("select * from testing2 where id=2;", {})
    print(cur.fetchone())

cur.execute("select * from testing2 where id=2;", {})

#print(con.load_data("testing"))
#print(con.collect_sql_quarry_result("select md5(?)", (b"foo",)))