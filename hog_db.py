#!/usr/bin/python3
# -*- coding: utf-8 -*-


import sys

import uuid
import datetime

LOG_FILE = ''.join(["db_ftrVect", datetime.datetime.now().strftime("_%H%M%S_"), uuid.uuid1().hex, ".log"])
f = None  # output file
DBG_PRINT = False
"""
The feature data is presented  as a dictionary with keyes 'ftr_name', 'ftr_file','ftr_value' 
"""


class imgFeature(object):
    """
    prepare data for db

    """

    def __init__(self, file_log_handler):
        self._dict = {"ftr_name": None,
                      "ftr_hval": "0xFFFFFFFF",
                      "ftr_file": None,
                      "ftr_value": None}

        self._flog = file_log_handler

    def set_feature(self, ftr_name, ftr_file, ftr_value, ftr_hval):

        self._dict["ftr_name"] = ftr_name
        self._dict["ftr_hval"] = ftr_hval
        self._dict["ftr_file"] = ftr_file
        self._dict["ftr_value"] = ftr_value

        if DBG_PRINT:
            print("feature data that is addding to DB ")

        if self._flog:
            print("feature data that is addind to DB ", file=self._flog)
        for key in self._dict:
            if DBG_PRINT:
                print("{}: {}".format(key, self._dict[key]))

            if self._flog:
                print("{}: {}".format(key, self._dict[key]), file=self._flog)

    def get_dict(self):
        return self._dict


class blmFilter(object):
    """

    """

    def __init__(self,  file_log_handler):

        self._dict = {"blm_size": None,
                      "blm_hashes": None,
                      "blm_bystr": None,
                      "blm_name": None}

        self._flog = file_log_handler


    def set_blm(self, filter_name, size, hash_count, byte_str):
        self._dict["blm_size"] = size
        self._dict["blm_hashes"] = hash_count
        self._dict["blm_bystr"] = byte_str     # byte_str is a string contains hex value of nibbles
        self._dict["blm_name"] = filter_name

        sMsg ="bloom filter  data that is addding to DB "

        if DBG_PRINT:
            print("{}".format(sMsg))

        if self._flog:
            print("{}".format(sMsg), file = self._flog )


        for key in self._dict:
            if DBG_PRINT:
                print("{}: {}".format(key, self._dict[key]) )

            if self._flog:
                print("{}: {}".format(key, self._dict[key]), file=self._flog )

    def get_dict(self):
        return self._dict

    def existindb(self, _db, _name):

        select_cmd = (
                    "SELECT blm_id, blm_name, blm_size, blm_hashes, blm_bystr FROM blmFilter WHERE blm_name='%s' " % _name)
        select_data = ()

        result = _db.__select_sql__(select_cmd, select_data)

        if len(result) == 0:
            return False

        print("{}".format(result))
        print("{}".format(result), file=self._flog)

        self._dict["blm_name"]   = result[0][1]
        self._dict["blm_size"]   = result[0][2]
        self._dict["blm_hashes"] = result[0][3]
        self._dict["blm_bystr"]  = result[0][4]        # string

        print("real size of byte string {} bytes vs expected size of bit array {} bits ( {} bytes)".format(
            len(self._dict["blm_bystr"]), self._dict["blm_size"], self._dict["blm_size"] / 4))
        if self._flog:
            print("real size of byte string {} bytes vs expected size of bit array {} bits ( {} bytes)".format(
                len(self._dict["blm_bystr"]), self._dict["blm_size"], self._dict["blm_size"] / 4), file=self._flog)

        if len(self._dict["blm_bystr"]) != self._dict["blm_size"] / 4:
            print(
                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  real size of byte string {} bytes is not equial  expected size of bit array {} bits ( {} bytes)".format(
                    len(self._dict["blm_bystr"]), self._dict["blm_size"], self._dict["blm_size"] / 4))
            if self._flog:
                print(
                    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   real size of byte string {} bytes is not equial  expected size of bit array {} bits ( {} bytes)".format(
                        len(self._dict["blm_bystr"]), self._dict["blm_size"], self._dict["blm_size"] / 4),
                    file=self._flog)

        print("dict {}".format(self._dict))
        print("dict {}".format(self._dict), file=self._flog)

        return True


class hogDB(object):
    """
     host - IP or DNS name. For testing host is 10.137.137.40 (gen-l-vrt-condor-master)
     database - . For testing database is imgTest
    """

    _flog = None

    def __init__(self, host, database, file_log_handler):
        """

        :param host: IP or DNS name. For testing host is 10.137.137.40 (gen-l-vrt-condor-master)
        :param database:  For testing database is imgTest
        :param file_log_handler:
        """
        super(hogDB, self).__init__()
        self._name = "db"
        self._insert_cnx = None
        self._host = host  # "10.137.137.40"   #gen-l-vrt-condor-master
        self._database = database  # "imgTest"
        self._user = "user"
        self._password = "123456"
        self._flog = file_log_handler

    def __del__(self):
        if self._insert_cnx:
            self._insert_cnx.close()

    def add_feature(self, feature_dict):
        """

        :param feature_dict: contains key:value pairs for all columns of features - table.
        Excluding ftr_id -auto-increnment field.

        :return: new value of ftr_id across call __insert_sql__()
        """

        ftr_name = feature_dict["ftr_name"]
        ftr_file = feature_dict["ftr_file"]
        ftr_value = feature_dict["ftr_value"]
        ftr_hval = feature_dict["ftr_hval"]

        insert_feature = ("INSERT INTO features "
                          "( ftr_name,  ftr_hval, ftr_file, ftr_value) "
                          "VALUES (%s,%s,%s, %s )")

        feature_data = (ftr_name, ftr_hval, ftr_file, ftr_value)

        if DBG_PRINT:
            print("insert feature {}\n feature_data {}".format(insert_feature, feature_data))

        if self._flog:
            print("insert feature {}\n feature_data {}".format(insert_feature, feature_data), file=self._flog)

        return self.__insert_sql__(insert_feature, feature_data)

    def add_blmFilter(self, blm_dict):
        """

        :param blm_dict:
        :return:
        """
        try:

            blm_size = blm_dict["blm_size"]
            blm_hashes = blm_dict["blm_hashes"]
            blm_bystr = blm_dict["blm_bystr"]
            blm_name = blm_dict["blm_name"]

            select_cmd = ("SELECT blm_id FROM blmFilter WHERE blm_name='%s' " % blm_name)
            select_data = ()

            if DBG_PRINT:
                print("bloom filter select {}\n ".format(select_cmd))

            if self._flog:
                print("bloom filter select {}\n ".format(select_cmd), file=self._flog)
            try:

                list_id = self.__select_sql__(select_cmd, select_data)
                if len(list_id) > 0:
                    blm_id = list_id[0][0]  # list_id is list ot tulpes
                else:
                    blm_id = -1

            except Exception as e:
                if DBG_PRINT:
                    print("select exception: {}".format(e))

                if self._flog:
                    print("select exception: {}".format(e), file=self._flog)

                return -1

            #blm_bystrstr = blm_bystr.decode(encoding="utf-16", errors="strict")




            insert_cmd = (
                "INSERT INTO blmFilter ( blm_name, blm_size, blm_hashes, blm_bystr ) VALUES ( %s, %s, %s, %s ) ")
            insert_data = (blm_name, blm_size, blm_hashes, blm_bystr)

            if DBG_PRINT:
                print("insert bloom filter {}\n blonm filter data {}".format(insert_cmd, insert_data))

            if self._flog:
                print("insert bloom filter {}\n blonm filter data {}".format(insert_cmd, insert_data), file=self._flog)

            #update_cmd = ("UPDATE blmFilter SET blm_size = %s , blm_hashes = %s , blm_bystr = %s WHERE blm_id = %s")
            #update_data = (blm_size, blm_hashes, blm_bystr, blm_id)

            update_cmd = ("UPDATE blmFilter SET  blm_bystr = %s WHERE blm_id = %s")
            update_data = (blm_bystr, blm_id)



            if DBG_PRINT:
                print("update bloom filter {}\n bloom filter data {}".format(update_cmd, update_data))

            if self._flog:
                print("update bloom filter {}\n bloom filter data {}".format(update_cmd, update_data), file=self._flog)
            # blm_bystr  analyze

            len_blm_bystr = len( blm_bystr )
            print("Bloom filter data: length of byte string {}\n {}".format(len_blm_bystr, blm_bystr))
            if self._flog:
                print("Bloom filter data: length of byte string {}\n {}".format(len_blm_bystr, blm_bystr),
                      file=self._flog)
            if len_blm_bystr != blm_size / 4:
                print("!!!!!!really length of byte string {} is not equal bit array length {} ( {} bytes)".format(
                    len_blm_bystr, blm_size, blm_size / 4))
                if self._flog:
                    print("!!!!!!really length of byte string {} is not equal bit array length {} ( {} bytes)".format(
                        len_blm_bystr, blm_size, blm_size / 4), file=self._flog)
            try:
                if blm_id < 0:
                    self.__insert_sql__(insert_cmd, insert_data)
                else:
                    self.__update_sql__(update_cmd, update_data)

            except Exception as e:
                if DBG_PRINT:
                    print("update/insert exception: {}".format(e))

                if self._flog:
                    print("update/insert exception: {}".format(e), file=self._flog)

                return -2
        except Exception as e0:
            pass
        finally:
            pass
        return blm_id


    def get_feature_row(self, ftr_id):
        ftr_name = None
        ftr_hval = None
        ftr_file = None

        select_cmd = ( "SELECT  ftr_id, ftr_name,ftr_hval,ftr_file, ftr_value  FROM features WHERE ftr_id = %s " % ftr_id )

        select_data = ()
        if DBG_PRINT:
            print("feature row select {}\n ".format(select_cmd))

        if self._flog:
            print("feature row  select {}\n ".format(select_cmd), file = self._flog )

        try:

            result = self.__select_sql__(select_cmd, select_data)
            ( ftr_id, ftr_name , ftr_hval, ftr_file, ftr_value ) = result[0]


        except Exception as e:
            if DBG_PRINT:
                print("select exception: {}".format(e))

            if self._flog:
                print("select exception: {}".format(e), file=self._flog)
        finally:
            if ftr_name:
                if DBG_PRINT:
                    print("ftr_name is {} ftr_hval is {} frt_file is {}".format( ftr_name, ftr_hval, ftr_file ) )

                if self._flog:
                    print("ftr_name is {} ftr_hval is {} frt_file is {}".format(ftr_name, ftr_hval, ftr_file), file =self._flog )

        return ftr_id, ftr_name, ftr_hval, ftr_file, ftr_value


    def get_ftrVect(self, ftr_id):

        ftr_value = None

        select_cmd = (
            "SELECT ftr_value FROM features WHERE ftr_id = %s " % ftr_id )
        select_data = ()

        if DBG_PRINT:
            print("ftr_value select {}\n ".format(select_cmd))

        if self._flog:
            print("ftr_value select {}\n ".format(select_cmd), file = self._flog )

        try:

            result = self.__select_sql__(select_cmd, select_data)
            ( ftr_value ) = result[0]

        except Exception as e:
            if DBG_PRINT:
                print("select exception: {}".format(e))

            if self._flog:
                print("select exception: {}".format(e), file=self._flog)
        finally:
            if ftr_value:
                if DBG_PRINT:
                    print("ftr_value is\n {} ".format( ftr_value ))

                if self._flog:
                    print("ftr_value is\n {} ".format(ftr_value), file =self._flog )

        return ftr_value

    def get_all_ids( self ):

        ftr_id_list=[]
        select_cmd = ("SELECT ftr_id FROM features ")
        select_data = ()

        if DBG_PRINT:
            print("ftr_id  select {}\n ".format(select_cmd))

        if self._flog:
            print("ftr_id select {}\n ".format(select_cmd), file = self._flog )

        try:

            result = self.__select_sql__(select_cmd, select_data)
            for i in range( len(result)):
                ftr_id_list.append( result[i ] )

        except Exception as e:
            if DBG_PRINT:
                print("select exception: {}".format(e))

            if self._flog:
                print("select exception: {}".format(e), file=self._flog)
        finally:
            if len( ftr_id_list ) >0 :
                if DBG_PRINT:
                    print("ftr_id is\n {} ... ".format( ftr_id_list[0]  ))

                if self._flog:
                    print("ftr_id list  is\n {} ".format(ftr_id_list), file =self._flog )

        return ftr_id_list

    def get_blmFilter(self, blm_name):

        select_cmd = (
                "SELECT blm_name, blm_size, blm_hashes, blm_bystr FROM blmFilter WHERE blm_name ='%s' " % blm_name)
        select_data = ()

        if DBG_PRINT:
            print("bloom filter select {}\n ".format(select_cmd))

        if self._flog:
            print("bloom filter select {}\n ".format(select_cmd), file=self._flog)

        try:
            result = self.__select_sql__(select_cmd, select_data)
            (blm_name, blm_size, blm_hashes, blm_bystr) = result[0]

        except Exception as e:
            if DBG_PRINT:
                print("select exception: {}".format(e))

            if self._flog:
                print("select exception: {}".format(e), file=self._flog)

            blm_size = None
            blm_hashes = None
            blm_bystr = None

        finally:
            if DBG_PRINT:
                print("name is {} size={} number of hashes={}\n bystr {}".format(blm_name, blm_size, blm_hashes,
                                                                                 blm_bystr))

            if self._flog:
                print("name is {} size={} number of hashes={}\n blm_bystr {}".format(blm_name, blm_size, blm_hashes,
                                                                                     blm_bystr), file=self._flog)

        return blm_name, blm_size, blm_hashes, blm_bystr

    def validation_saved_data(self, blm_dict):
        try:
            ndiffs = 0
            blm_name1, blm_size1, blm_hashes1, blm_bystr1 = self.get_blmFilter(blm_dict["blm_name"])

            #blm_bystr1 = bytearray( blm_bystrstr,'utf-8' )

            if blm_name1 != blm_dict["blm_name"]:

                ndiffs += 1
                print("the name is differ: {}  vs {}".format(blm_name1, blm_dict["blm_name"]))
                if self._flog:
                    print("the name is differ: {}  vs {}".format(blm_name1, blm_dict["blm_name"]), file=self._flog)

            if blm_size1 != blm_dict["blm_size"]:
                ndiffs += 1
                print("the size is differ: {}  vs {}".format(blm_size1, blm_dict["blm_size"]))
                if self._flog:
                    print("the size is differ: {}  vs {}".format(blm_size1, blm_dict["blm_size"]), file=self._flog)

            if blm_hashes1 != blm_dict["blm_hashes"]:
                ndiffs += 1
                print("the number of hashes is differ: {}  vs {}".format(blm_hashes1, blm_dict["blm_hashes"]))
                if self._flog:
                    print("the number of hashesis differ: {}  vs {}".format(blm_hashes1, blm_dict["blm_hashes"]),
                          file=self._flog)

            blm_bystr0 = blm_dict["blm_bystr"]

            if len(blm_bystr1) != len(blm_bystr0):
                ndiffs += 1
                print("the length of byte arrays  is differ: {}  vs {}".format(len(blm_bystr1), len(blm_bystr0)))
                if self._flog:
                    print("the length of byte arrays  is differ: {}  vs {}".format(len(blm_bystr1), len(blm_bystr0)),
                          file=self._flog)


            if blm_bystr1 != blm_bystr0 :
                ndiffs+=1
                print("the {} th string are differ: {} \n vs \n{}".format( blm_bystr1, blm_bystr0))
                if self._flog:
                    print("the {} th string are differ: {} \n vs\n {}".format( blm_bystr1, blm_bystr0),file=self._flog)

            print("{} differs".format(ndiffs))
        except Exception as e:
            print("Exception in validation... {}".format(e))
            if self._flog:
                print("Exception in validation... {}".format(e), file=self._flog)
        finally:
            pass
        if self._flog:
            print("{} differs".format(ndiffs), file=self._flog)

        return ndiffs

    def __insert_sql__(self, insert_cmd, insert_data):

        lastrowid = None  # type: None
        if self._insert_cnx is None:
            import mysql.connector

            try:
                self._insert_cnx = mysql.connector.connect(user=self._user, password=self._password, host=self._host,
                                                           database=self._database, charset='utf8',
                                                           collation='utf8_general_ci')

            except Exception as e:
                if DBG_PRINT:
                    print("exception: {}".format(e))

                if self._flog:
                    print("Exception: {}".format(e), file=self._flog)

        try:
            cursor = self._insert_cnx.cursor()
            if DBG_PRINT:
                print("insert cmd {}\n insert_data {}".format(insert_cmd, insert_data))

            if self._flog:
                print("insert cmd {}\n insert_data {}".format(insert_cmd, insert_data), file=self._flog)
            # encode
            insert_cmd1 = insert_cmd.encode(encoding='UTF-8', errors='strict')
            insert_data1 = insert_data

            cursor.execute(insert_cmd1, insert_data1)
            lastrowid = cursor.lastrowid
            self._insert_cnx.commit()
            cursor.close()
        except Exception as e:
            if DBG_PRINT:
                print("exception: {}check type of insert".format(e))

            if self._flog:
                print("Exception: {} check type of insert".format(e), file=self._flog)
            lastrowid = -1
        finally:
            pass
        return lastrowid

    def __select_sql__(self, select_cmd, select_data):
        import mysql.connector

        try:
            cnx = mysql.connector.connect(user=self._user, password=self._password, host=self._host,
                                          database=self._database, charset='utf8', collation='utf8_general_ci')
        except Exception as e:
            if DBG_PRINT:
                print("connector exception: {}".format(e))

            if self._flog:
                print("connector exception: {}".format(e), file=self._flog)

            return []
        try:
            cursor = cnx.cursor()
            # encode
            select_cmd1 = select_cmd.encode(encoding='UTF-8', errors='strict')

            select_data1 = select_data
            cursor.execute(select_cmd1, select_data1)
            result = cursor.fetchall()
            cursor.close()
            cnx.close()
        except Exception as e:
            if DBG_PRINT:
                print("cursor exception:  {}".format(e))

            if self._flog:
                print("cursor exception: {}".format(e), file=self._flog)
            result = []
        finally:
            pass
        return result

    def __update_sql__(self, update_sql, update_data):
        import mysql.connector
        ret_value = 1  # type: int
        try:
            cnx = mysql.connector.connect(user=self._user, password=self._password, host=self._host,
                                          database=self._database, charset='utf8', collation='utf8_general_ci')
        except Exception as e:
            if DBG_PRINT:
                print("connector exception: {}".format(e))

            if self._flog:
                print("connector exception: {}".format(e), file=self._flog)

            return -1

        try:
            cursor = cnx.cursor()

            update_sql1 = update_sql.encode(encoding='UTF-8', errors='strict')
            update_data1 = update_data
            cursor.execute(update_sql1, update_data1)

            cursor.close()
            ret_value = 0

        except Exception as e:
            if DBG_PRINT:
                print("cursor exception:  {} check type of update".format(e))

            if self._flog:
                print("cursor exception: {} check type of update".format(e), file=self._flog)
        finally:
            cnx.commit()
            cnx.close()

        return ret_value


def main(arg_list):
    print(arg_list)
    _host = "10.137.137.40"
    _database = "imgTest"
    db = hogDB(_host, _database, f)
    feature = imgFeature(f)

    feature.set_feature("photo1", "pedestran/535.png", "aabbccdd001122334455667788")

    ftr_id = db.add_feature(feature._dict)

    print("feature id = {}".format(ftr_id))

    db.__del__()


if __name__ == "__main__":

    if DBG_PRINT:
        print("The log file  =  {}".format(LOG_FILE))

    f = open(LOG_FILE, 'w')

    ret = main(sys.argv)

    if DBG_PRINT:
        print("return code: {}".format(ret))

    print("return code: {}".format(ret), file=f)

    f.close()

    sys.exit(ret)
