#!/usr/bin/python3
# -*- coding: utf-8 -*-


import os
import sys

from hog import hog, list_img_files
from bloomfilter import BloomFilter, bitarray2string, string2bitarray
from hog_db import LOG_FILE, DBG_PRINT,  hogDB, imgFeature, blmFilter
from hog_statest import hog_statest


def exit_with_code(code, file_log=None, dbg_prn = None):
    _fname = exit_with_code.__name__
    if dbg_prn:
        print("##### Function ...{} started".format(_fname))
    try:
        if code == -1:
            sMsg = "####### exit due to database connection fail"
        elif code == -2:
            sMsg = "####### exit due to Bloom filter creating fail"
        elif code == -99:
            sMsg = "####### exit due to command line parameters"
        else:
            sMsg = "####### exit "

        print(sMsg)
        if file_log:
            print(sMsg, file=file_log)

    except Exception as e:
        print("### Exception in {} : {}".format(_fname, e))
        if file_log:
            print("### Exception in {} : {}".format(_fname, e), file=file_log)
    finally:
        if file_log:
            file_log.close()
        if dbg_prn:
            print("##### Function ...{} finished\n".format(_fname))

    sys.exit(code)


def parseCMD(arglist, file_log=None, dbg_prn=None):
    """

    :param arglist:
             host,
             database,
             name of data chunck in DB,
             path to images,
             number of images max and false probability are using for Bloom filter configuration
             trust parameter for Bloom filter
    :param file_log:  handler for log file
    :param dbg_prn:  debug prints
    :return:
    """
    _fname = parseCMD.__name__
    if dbg_prn:
        print("##### Function ...{} started".format(_fname))
    try:

        print("#####{}\n".format(arglist))
        if file_log:
            print("#####{}\n".format(arglist), file=file_log)

        for i in range(len(arglist)):
            if dbg_prn:
                print("##### arg[{}]={}".format(i, arglist[i]))
            if file_log:
                print("##### arg[{}]={}".format(i, arglist[i]), file=file_log)

        # path_base = "/home/osboxes/PycharmProjects/hog_data/pedestrian/"
        _host = arglist[1]
        _database = arglist[2]
        _NAME = arglist[3]
        _path_base = arglist[4]
        _N_IMAGES = arglist[5]
        _PB = arglist[6]
        _TRUST = arglist[7]    # 'trust' or 'trustless'

    except Exception as e:
        print("### Exception in {} : {}".format(_fname, e))
        if file_log:
            print("### Exception in {} : {}".format(_fname, e), file=file_log)
        _host = None
        _database = None
        _NAME = None
        _path_base = None
        _PB = None
        _TRUST='trustless'

    finally:
        if dbg_prn:
            print("##### Function ...{} finished\n".format(_fname))

    return _host, _database, _NAME, _path_base, _N_IMAGES, _PB, _TRUST


def init_DB(_host, _database, file_log=None, dbg_prn=None):
    _fname = init_DB.__name__
    if dbg_prn:
        print("##### Function ...{} started".format(_fname))
    try:

        _db = hogDB(_host, _database, file_log)

    except Exception as e:
        print("### Exception in {} : {}".format(_fname, e))
        if file_log:
            print("### Exception in {} : {}".format(_fname, e), file=file_log)
        _db = None

    finally:
        if dbg_prn:
            print("##### Function ...{} finished\n".format(_fname))

    return _db


def init_BloomFilter(_nameBloomFilter, _db, _n_images, _pb,  file_log=None, dbg_prn=None):
    """

    :param _nameBloomFilter:
    :param _db:
    :param _n_images:
    :param _pb:
    :param file_log:
    :param dbg_prn:
    :return:
    """

    _fname = init_BloomFilter.__name__
    if dbg_prn:
        print("##### Function ...{} started".format(_fname))

    f =file_log

    try:
        _NAME = _nameBloomFilter
        _N_IMAGES = _n_images
        _PB = _pb


        _blmFilterDB = blmFilter( file_log )  # interface to table 'blmFilter' in 'imgTest' database

        if _blmFilterDB.existindb(_db, _NAME):  # exists in DB

            dict_for_blmfilter = _blmFilterDB.get_dict()
            string_dataset_from_db = dict_for_blmfilter["blm_bystr"]
            bitarray_size = dict_for_blmfilter["blm_size"]
            hash_count_from_db = dict_for_blmfilter["blm_hashes"]

            _blmFilter = BloomFilter(_N_IMAGES, _PB, f, None, string_dataset_from_db, bitarray_size, hash_count_from_db)


        else:  # new, it should be added to DB

            _blmFilter = BloomFilter(_N_IMAGES, _PB, f)  # type: object


            _, _, blm_bystr = _blmFilter._filter_array_for_storing()

            _blmFilterDB.set_blm(_NAME, _blmFilter.get_size(), _blmFilter.get_hash_count(), blm_bystr)

            _db.add_blmFilter(_blmFilterDB.get_dict())





    except Exception as e:
        print("### Exception in {} : {}".format(_fname, e))
        if file_log:
            print("### Exception in {} : {}".format(_fname, e), file=file_log)
        _blmFilter = None
        _blmFilterDB = None

    finally:
        if dbg_prn:
            print("##### Function ...{} finished\n".format(_fname))

    return _blmFilter, _blmFilterDB


def get_FilterProperties(_db, _name_blm_filter, file_log=None, dbg_prn=None):
    _fname = get_FilterProperties.__name__
    if dbg_prn:
        print("##### Function ...{} started".format(_fname))

    _NAME = _name_blm_filter
    try:

        blm_name, blm_size, blm_hashes, blm_bystr = _db.get_blmFilter(_NAME)
        if dbg_prn:
            print("####### blm_name ={}, blm_size={}, blm_hashes={}, blm_bystr= .... ".format(blm_name, blm_size,
                                                                                              blm_hashes))
        if file_log:
            print("####### blm_name ={}, blm_size={}, blm_hashes={}\n ####### blm_bystr=\n{}".format(blm_name, blm_size,
                                                                                                     blm_hashes,
                                                                                                     blm_bystr),
                  file=file_log)
    except Exception as e:
        print("### Exception in {} : {}".format(_fname, e))
        if file_log:
            print("### Exception in {} : {}".format(_fname, e), file=file_log)
        blm_name = None
        blm_size = None
        blm_hashes = None
        blm_bystr = None

    finally:
        if dbg_prn:
            print("##### Function ...{} finished\n".format(_fname))

    return blm_name, blm_size, blm_hashes, blm_bystr

def get_img_list( _path_base, file_log=None, dbg_prn=None):
    _fname = get_img_list.__name__
    if dbg_prn:
        print("##### Function ...{} started".format(_fname))
    try:

        train_images = list_img_files(_path_base)
        number_of_images = len(train_images)

    except Exception as e:
        print("### Exception in {} : {}".format(_fname, e))
        if file_log:
            print("### Exception in {} : {}".format(_fname, e), file=file_log)
        number_of_images=0
        train_images=[]

    finally:
        if dbg_prn:
            print("##### Function ...{} finished\n".format(_fname))

    return number_of_images, train_images


def img_iteration(_host, _database,  number_of_images, train_images, _blmFilter, _blmFilterDB, _NAME, file_log=None,
                  dbg_prn=None):

    """

    :param _host:  host
    :param _database:
    :param number_of_images:
    :param train_images:
    :param _blmFilter:
    :param _blmFilterDB:
    :param _NAME:
    :param file_log:
    :param dbg_prn:
    :return: result_dict{"added":X,"existed":Y,"discarded":Z}, existed_dict{"ftr_name":"ftr_hval"}
    """
    _fname = img_iteration.__name__
    if dbg_prn:
        print("##### Function ...{} started".format(_fname))

    result_dict =  {}
    existed_dict = {}
    try:
        db = init_DB(_host, _database, file_log, dbg_prn)
        image_count = 0
        added_to_filter = 0
        existed_in_filter = 0
        discarded = 0

        for fname in train_images:
            if dbg_prn:
                print("******** {} image from {} processing started.... {}".format(image_count, number_of_images, fname))
            if file_log:
                print("\n******** {} image from {} processing started.... {}".format(image_count, number_of_images, fname),
                  file=file_log )

            fimage = hog_statest(fname, 64, 128, _host, _database, file_log )
            try:
                fimage._run()
                fimage._ftr_vect2string()

                should_be_added =  _blmFilter._add( fimage._str_fvect )

                if should_be_added :
                    fimage._save_fvect()
                    added_to_filter += 1

                    if dbg_prn:
                        print("******** The feature vector for {}  added to Bloom filter data".format(fname))
                    if file_log:
                        print("******** The feature vector for {}  added to Bloom filter data".format(fname),
                              file=file_log)
                else:
                    if _blmFilter.trust_or_trustless == 'trust':

                        existed_in_filter += 1
                        if dbg_prn:
                            print("******** The feature vector for {}  exists  in Bloom filter data".format(fname))
                        if file_log:
                            print("The feature vector for {}  exists  in Bloom filter data".format(fname), file=file_log)

                        imgFeature = fimage.getimgFeature()
                        img_dict = imgFeature.get_dict()
                        existed_dict.update({img_dict["ftr_name"]: img_dict["ftr_hval"]})
                    else:
                        fimage._save_fvect()
                        added_to_filter += 1
                        if dbg_prn:
                            print("******** Trustless strategy: the feature vector for {}  added to Bloom filter data".format(fname))
                        if file_log:
                            print("******** Trustless strategy: the feature vector for {}  added to Bloom filter data".format(fname),
                                  file=file_log)



            except Exception as e:
                print("### Exception on image {}".format(fname))
                if file_log:
                    print("### Exception on image {}".format(fname), file=file_log)
                discarded = discarded + 1
                del fimage
                continue

            del fimage

            if image_count % 10  == 0:    # save bitarray


                _, _, blm_str = _blmFilter._filter_array_for_storing()



                #for i in range( len(blm_str) ):
                #    if (blm_str[i] != 0):
                #      print ("{} {}".format(i, blm_str[i]))



                _blmFilterDB.set_blm( _NAME, _blmFilter.get_size(), _blmFilter.get_hash_count(), blm_str )

                db.add_blmFilter( _blmFilterDB.get_dict() )

                if db.validation_saved_data( _blmFilterDB.get_dict() ) > 0:
                    print(" Validation for adding bloom filter failed!!!!!")
                    if f:
                        print(" Validation for adding bloom filter failed!!!!!", file=f )
                    f.close()
                    sys.exit(-1)

                if file_log:
                    print("\n{}".format(_blmFilter.get_bit_array), file=file_log )
                if dbg_prn:
                    print("******** Bloom {} bit array saved after {} image processed".format(_NAME, image_count))
                if file_log:
                    print("******** Bloom {} bit array saved after {} image processed".format(_NAME, image_count),
                          file=file_log)

            if dbg_prn:
                print("******** {} image from {} processing finished\n\n".format(image_count, number_of_images))
            if file_log:
                print("******** {} image from {} processing finished\n\n".format(image_count, number_of_images), file=file_log)
            image_count = image_count + 1

        #del _blmFilter
        #del _blmFilterDB

        if dbg_prn:
            print("\n\n####### Passed : {} images\n added: {}\n existed: {}\n discarded: {}".format(number_of_images, added_to_filter,
                                                                                    existed_in_filter, discarded))
        if file_log:
            print("\n\n####### Passed : {} images\n added: {}\n existed: {}\n discarded: {}".format(number_of_images, added_to_filter,
                                                                                    existed_in_filter, discarded),
              file=file_log)



        result_dict.update( {"added" : added_to_filter} )
        result_dict.update( {"existed" : existed_in_filter } )
        result_dict.update( {"discarded" : discarded} )


    except Exception as e:
        print("### Exception in {} : {}".format(_fname, e))
        if file_log:
            print("### Exception in {} : {}".format(_fname, e), file=file_log)


    finally:
        if dbg_prn:
            print("##### Function ...{} finished\n".format(_fname))

    return result_dict, existed_dict


def main(arglist, f, dbg_prn):
    """
    flow for adding the feature vector of the image to DB
    :param arglist:
    :return:
    """

    _host, _database, _NAME, _path_base, _N_IMAGES, _PB, _TRUST = parseCMD( arglist, f, dbg_prn )


    if _host is None or _database is None or _path_base is None:
        exit_with_code(-99, f, dbg_prn)

    # _host = "10.137.137.40"
    # _database = "imgTest"

    db = init_DB(_host, _database, f, dbg_prn)
    if db is None:
        exit_with_code(-1, f, dbg_prn)

    # set Bloom filter
    try:

        _blmFilter, _blmFilterDB = init_BloomFilter(_NAME, db, _N_IMAGES, _PB, f, dbg_prn)

        if _blmFilter is None or _blmFilterDB is None:
            exit_with_code(-2, f, dbg_prn)

        #set Bloom filter behavior

        _blmFilter.trust_or_trustless = _TRUST

        blm_name, blm_size, blm_hashes, blm_bystr = get_FilterProperties(db, _NAME, f, dbg_prn)

    except Exception as e:
        print("### Exception in main : {}".format(e))
        if f:
            print("### Exception in main : {}".format(e), file=f)

        exit_with_code(-95, f, dbg_prn)



    train_images = list_img_files(_path_base)

    number_of_images = len(train_images)

    number_of_images, train_images = get_img_list(_path_base, f, dbg_prn )

    if number_of_images == 0:
        print("The image folder is empty : {}".format(number_of_images))
        if f:
            print("The image folder is empty : {}".format(number_of_images), file=f)

        del _blmFilter
        del _blmFilterDB
        exit_with_code(-50, f, dbg_prn)



    try:

        img_iteration( _host, _database,  number_of_images,  train_images,  _blmFilter,  _blmFilterDB,  _NAME, f, dbg_prn)

    except Exception as e:
        print("### Exception in main : {}".format( e))
        if f:
            print("### Exception in main : {}".format( e), file=f )

        exit_with_code(-98, f, dbg_prn)
    finally:

        del _blmFilter
        del _blmFilterDB



    return 0


if __name__ == "__main__":

    """

    The command line is following
    ./hog_statest.py 10.137.137.40 imgTest Second_attempt /home/osboxes/PycharmProjects/hog_data/pedestrian/ 10000 0.05

    """

    if DBG_PRINT:
        print("The log file  =  {}".format(LOG_FILE))

    f = open(LOG_FILE, 'w')

    ret = main(sys.argv, f,DBG_PRINT)

    if DBG_PRINT:
        print("return code: {}".format(ret))

    print("return code: {}".format(ret), file=f)

    f.close()

    sys.exit(ret)
