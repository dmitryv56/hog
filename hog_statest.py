#!/usr/bin/python3
# -*- coding: utf-8 -*-


# Copyright (C) Mellanox Technologies, Ltd. 2019-2019. ALL RIGHTS RESERVED.
#
# This software product is a proprietary product of Mellanox Technologies, Ltd.(the "Company") and all right, title, and
# interest in and to the software product, including all associated intellectual property rights, are and shall remain
# exclusively with the Company. All rights in or to the software product are licensed, not sold.
# All rights not licensed are reserved.
#
# This software product is governed by the End User License Agreement provided with the software product.
#


import os
import sys
import numpy as np

from hog import hog, list_img_files
from bloomfilter import BloomFilter
from hog_db import LOG_FILE, DBG_PRINT, f, hogDB, imgFeature, blmFilter

"""
_N_IMAGES = 10000  # type: int
_PB = 0.05
_NAME = "First attempt"
_HOST = "10.137.137.40"
_DATABASE = "imgTest"

"""


class hog_statest(hog):
    """
   classdoc
   """

    def __init__(self, path2img, N, M, host, database, file_log_handler, channel=0):
        """

        :type path2img: object
        """
        super().__init__(path2img, N, M, channel)

        self._str_fvect = None

        self._id = None

        _, self._name = os.path.split(path2img)

        self._image_file = path2img

        self._file_log_handler = file_log_handler

        self._db = hogDB(host, database, self._file_log_handler)

        self._imgFeature = imgFeature(self._file_log_handler)

    def getimgFeature(self):
        return self._imgFeature


    def _run(self):
        try:
            self._read_image()  # read image
            self._resize()

            if self._num_of_channels == 1:
                self._grad_images(0)
                self._grad()
                self._fvect(0)
                self._final_feature_vector()
            elif self._num_of_channels == 3:

                for iChannel in range(self._img.shape[2]):
                    self._grad_images(iChannel)
                    self._grad()
                    self._fvect(iChannel)

                self._final_feature_vector()

            else:
                print("hog_statest._run() invalid number of color channels")
                if self._file_log_handler:
                    print("hog_statest._run() invalid number of color channels", file=self._file_log_handler)
        except Exception as e:
            print ("Exception in hog_statest._run() {}".format(e))
            if self._file_log_handler:
                print("Exception in hog_statest._run() {}".format(e), file = self._file_log_handler )
        finally:
            pass
        return

    def get_hash32(self):

        if self._hash32:

            print(" 32 bit hash ={}".format(self._hash32))
            print(" 32 bit hash ={}".format(self._hash32), file=self._file_log_handler)

            return self._hash32

        if self._str_fvect is None:
            self._hash32 = "0xFFFFFFFF"
            return self._hash32


        self._hash32 = hash32_calc( self._str_fvect, len( self._str_fvect), self._file_log_handler )

        return self._hash32


    def _ftr_vect2string(self):
        """
        creates str for feature vector and creates 32 bit hash for feature vector
        :return:
        """

        self._str_fvect = hog.vec2hexstr(8, self._final_fvect)
        self.get_hash32()





    def _save_fvect(self):

        try:
            #self._str_fvect = hog.vec2hexstr(8, self._final_fvect)
            #self.get_hash32()
            self._imgFeature.set_feature(self._name, self._image_file, self._str_fvect, self._hash32 )
            self._db.add_feature(self._imgFeature.get_dict())
        except Exception as e:
            print("Exception {}".format(e))
            print("Exception {}".format(e), file = self._file_log_handler)
        finally:
            pass


def hash32_calc( fvect, size_fvect, file_log_handler ):
    """
    seed^=i+ 0x9e3779b9 + (seed<<6) + (seed>>2)
    where seed = vector.size
           i iterates a values in vector

    Due to problem with scalar type, we define seed -array of 1-size and use first element
    :param fvect:  - byte vector
    :param size_fvect: size of byte vector
    :return: seed
    """
    seed=np.zeros(1, dtype = np.uint32 )
    magic=0x9e3779b9
    uia32 = np.zeros( int(size_fvect/4), dtype=np.uint32 )
    try:
        k = 0
        for i in range(0, size_fvect, 4 ):
            svalue  = "0x{:02X}".format( int(fvect[i], 16)  ) + "{:02X}".format( int(fvect[i + 1 ], 16 ) ) +\
                      "{:02X}".format( int(fvect[i + 2 ], 16) )+ "{:02X}".format( int(fvect[i + 3 ],16) )

            uia32[ k ] = int( svalue, 16 )
            k+=1

        for aa in uia32:
            seed[0] ^= (aa + magic + (seed[0] << 6) + (seed[0] >> 2))

            #print("0x{:8X}".format(seed[0]))

    except Exception as e:
        print("Exception {}".format(e))
        if file_log_handler:
            print("Exception {}".format(e), file = file_log_handler )
    finally:
        pass
    sret =  "0x{:08X}".format( seed[0] )
    return sret


def main(arglist):
    """
    flow for add feature vector of the image to DB
    :param arglist:
    :return:
    """
    print("\n{}\n".format(arglist))
    print("\n{}\n".format(arglist), file=f)

    for i in range(len(arglist)):
        print("arg[{}]={}".format(i, arglist[i]))
        print("arg[{}]={}".format(i, arglist[i]), file=f)

    # path_base = "/home/osboxes/PycharmProjects/hog_data/pedestrian/"
    _host = arglist[1]
    _database = arglist[2]
    _NAME = arglist[3]
    _path_base = arglist[4]
    _N_IMAGES = arglist[5]
    _PB = arglist[6]
    _TRUST='trustless'

    # _host = "10.137.137.40"
    # _database = "imgTest"
    db = hogDB(_host, _database, f)

    # set Bloom filter

    _blmFilterDB = blmFilter( f)

    if _blmFilterDB.existindb(db, _NAME):  # exists in DB

        dict_for_blmfilter = _blmFilterDB.get_dict()
        bystr_dataset_from_db = dict_for_blmfilter["blm_bystr"]
        bitarray_size = dict_for_blmfilter["blm_size"]
        hash_count_from_db = dict_for_blmfilter["blm_hashes"]

        _blmFilter = BloomFilter(_N_IMAGES, _PB, f, None, bystr_dataset_from_db,
                                 bitarray_size, hash_count_from_db)
    else:  # new, it should be added to DB
        _blmFilter = BloomFilter(_N_IMAGES, _PB, f)  # type: object

        _, _, blm_bystr = _blmFilter._filter_array_for_storing()

        _blmFilterDB.set_blm(_NAME, _blmFilter.get_size(), _blmFilter.get_hash_count(), blm_bystr)

        db.add_blmFilter(_blmFilterDB.get_dict())

    # get filter data from DB
    blm_name, blm_size, blm_hashes, blm_bystr = db.get_blmFilter(_NAME)
    print("blm_name ={}, blm_size={}, blm_hashes={}, blm_bystr=\n{}".format(blm_name, blm_size, blm_hashes, blm_bystr))

    # re-create bloom filter

    # bitarray_size, hash_count, bitarray_dataset = BloomFilter._filter_array_from_storing(blm_size, blm_hashes,blm_bystr)

    # _blmFilter1 = BloomFilter(_N_IMAGES, _PB, f, bitarray_dataset, bitarray_size)

    # del _blmFilter

    train_images = list_img_files(_path_base)
    number_of_images = len(train_images)
    image_count = 0
    added_to_filter = 0
    existed_in_filter = 0
    discarded = 0

    for fname in train_images:
        print("\n**** {} image from {} processing started.... {}".format(image_count, number_of_images, fname))
        print("\n**** {} image from {} processing started.... {}".format(image_count, number_of_images, fname), file=f)

        fimage = hog_statest(fname, 64, 128, _host, _database, f)
        try:
            fimage._run()
            fimage._save_fvect()
        except Exception as e:
            print("exception on image {}".format(fname))
            print("exception on image {}".format(fname), file=f)
            discarded = discarded + 1
            del fimage
            continue

        if _blmFilter._add(fimage._str_fvect):
            print("The feature vector for {}  added to Bloom filter data".format(fname))
            print("The feature vector for {}  added to Bloom filter data".format(fname), file=f)
            added_to_filter = added_to_filter + 1
        else:
            print("The feature vector for {}  exists  in Bloom filter data".format(fname))
            print("The feature vector for {}  exists  in Bloom filter data".format(fname), file=f)
            existed_in_filter = existed_in_filter + 1
        del fimage

        if image_count % 10 == 0:  # save bitarray
            _, _, blm_bystr = _blmFilter._filter_array_for_storing()

            _blmFilterDB.set_blm(_NAME, _blmFilter.get_size(), _blmFilter.get_hash_count(), blm_bystr)

            db.add_blmFilter(_blmFilterDB.get_dict())
            print("\n{}".format(_blmFilter.get_bit_array), file=f)

            if  db.validation_saved_data( _blmFilterDB.get_dict() ) >0 :
                print ( " Validation for adding bloom filter failed!!!!!")
                if f:
                    print(" Validation for adding bloom filter failed!!!!!", file = f )
                f.close()
                sys.exit(-1)






            print("Bloom {} bit array saved after {} image processed".format(_NAME, image_count))
            print("Bloom {} bit array saved after {} image processed".format(_NAME, image_count), file=f)

        print("\n**** {} image from {} processing finished\n\n".format(image_count, number_of_images))
        print("\n**** {} image from {} processing finished\n\n".format(image_count, number_of_images), file=f)
        image_count = image_count + 1

    del _blmFilter
    del _blmFilterDB

    print("passed : {} images\n added: {}\n existed: {}\n discarded: {}".format(number_of_images, added_to_filter, existed_in_filter, discarded ))
    print("passed : {} images\n added: {}\n existed: {}\n discarded: {}".format(number_of_images, added_to_filter, existed_in_filter, discarded ),
          file=f)

    return 0


if __name__ == "__main__":

    """
    
    The command line is following
    ./hog_statest.py 10.137.137.40 imgTest Second_attempt /home/osboxes/PycharmProjects/hog_data/pedestrian/ 10000 0.05
       
    """

    if DBG_PRINT:
        print("The log file  =  {}".format(LOG_FILE))

    f = open(LOG_FILE, 'w')


    # degug function area
    # sizestr=len(strfvect)
    # fvect=np.zeros(int(sizestr/2))
    # k=0
    # for i in range(0,sizestr,2):
    #     fvect[k]=int(strfvect[i:i+2],16)
    #     k+=1
    # size_fvect=sizestr/2
    # hsh = hash32_calc(fvect, int(size_fvect))
    # pass
    #

    ret = main(sys.argv)

    if DBG_PRINT:
        print("return code: {}".format(ret))

    print("return code: {}".format(ret), file=f)

    f.close()

    sys.exit(ret)
