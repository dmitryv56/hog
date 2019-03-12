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


import math
import mmh3

# pip install mmh3
# pip install bitarray
from bitarray import bitarray


def bitarray2string(a_bitarray):

    k=0
    while len(a_bitarray)%8 !=0 and k <10 :
        a_bitarray.append(False)
        k+=1


    ss=a_bitarray.to01()

    str_bitarray=""

    for i in range(0,len(ss),8):
        str_bitarray= str_bitarray + "{:02X}".format(int(ss[i:i+8],2 ))
    return str_bitarray

def string2bitarray(str_bitarray):
    """

    :rtype: object
    """
    l=len(str_bitarray)

    a_bitarray=bitarray( l * 4 )

    for i in range(len(str_bitarray)):
        start=i*4

        a_bitarray[start] = a_bitarray[start+1] = a_bitarray[start+2] = a_bitarray[start+3] = False

        if str_bitarray[i]  =='1':
            a_bitarray[ start + 3 ]=True
        elif str_bitarray[i]=='2':
            a_bitarray[ start + 2 ] = True
        elif str_bitarray[i]=='3':
            a_bitarray[ start + 2 ] = a_bitarray[ start + 3 ] = True
        elif str_bitarray[i]=='4':
            a_bitarray[start + 1] = True
        elif str_bitarray[i]=='5':
            a_bitarray[start + 1] = a_bitarray[start +3 ] = True
        elif str_bitarray[i]=='6':
            a_bitarray[start + 1] = a_bitarray[start + 2] = True
        elif str_bitarray[i]=='7':
            a_bitarray[start + 1] = a_bitarray[start + 2] = a_bitarray[start + 3] = True
        elif str_bitarray[i]=='8':
            a_bitarray[ start ] = True
        elif str_bitarray[i]=='9':
            a_bitarray[ start ] = a_bitarray[start + 3] = True
        elif str_bitarray[i]=='A' or str_bitarray[i]=='a':
            a_bitarray[start] = a_bitarray[start + 2] = True
        elif str_bitarray[i]=='B' or str_bitarray[i]=='b':
            a_bitarray[start] = a_bitarray[start + 2] =a_bitarray[ start + 3 ] = True

        elif str_bitarray[i]=='C' or str_bitarray[i]=='c':
            a_bitarray[start] = a_bitarray[start + 1]  = True

        elif str_bitarray[i]=='D' or str_bitarray[i]=='d':
            a_bitarray[start] = a_bitarray[start + 1] =a_bitarray[ start + 3] = True
        elif str_bitarray[i]=='E' or str_bitarray[i]=='e':
            a_bitarray[start] = a_bitarray[start + 1] = a_bitarray[start + 2] = True
        elif str_bitarray[i]=='F' or str_bitarray[i]=='f':
            a_bitarray[start] = a_bitarray[start + 1] = a_bitarray[start + 2] = a_bitarray[ start ] = True
    k=0
    while len(a_bitarray)%8 !=0 and k <10:
        a_bitarray.append(False)
        k+=1

    return a_bitarray


class BloomFilter(object):
    """
    Class for BloomFilter, using murmur3 hash functions.
    www.geeksforgeeks.org/bloom-filters-introduction-and-python-implementation

    """

    def __init__(self, items_count, fp_prob, file_handler, bitarray_dataset=None, string_dataset_from_db = None,
                 bitarray_size=None, hash_count_from_db=None ):
        """

        :param items_count: int
               Number of items expected to be stored in bloom filter
        :param fp_prob: float
                False positive probability in decimal
        :param file_handler - log file handler
        :param bitarray_dataset: bitarray
                bit array for existing Bloom filter or None for new filter
        :param string_dataset_from_db - string from database
        :param bitarray_size for existing Bloom filter
        :param hash_count_from_db - hash count from db, if bystr_dataset_from_db is not none

        """

        self._fp_prob = float(fp_prob)

        self._flog = file_handler

        if bitarray_size is None:
            self._size = int( self._get_size( int(items_count), self._fp_prob))
        else:
            self._size = int( bitarray_size )

        if hash_count_from_db is None:
            self._hash_count = self._get_hash_count( self._size, int(items_count) )
        else:
            self._hash_count = int( hash_count_from_db )

        if bitarray_dataset is None and string_dataset_from_db is None:
            self._bit_array = bitarray( self._size )
            self._bit_array.setall(0)

        elif string_dataset_from_db is None and bitarray_dataset:
            self._bit_array = bitarray_dataset.copy()

        elif string_dataset_from_db  and bitarray_dataset is None:
            self._bit_array = string2bitarray( string_dataset_from_db )
        else:
            self._bit_array = bitarray( self._size )
            self._bit_array.setall( 0 )


    def get_size(self):
        return self._size

    def get_hash_count(self):
        return self._hash_count

    def get_bit_array(self):
        return self._bit_array

    def _add(self, item):
        """
        Add item in the filter
        :param item:
        :return:
        """
        _added = False
        digests = []
        for i in range(self._hash_count):
            # create digest for given item
            # 'i' works as seed to mmh3.hash() function
            # with different seed , digest created is different

            digest = mmh3.hash(item, i) % self._size
            digests.append(digest)
            print("digest ={} length (bit_array) ={}".format(digest, self._bit_array.length() ), file = self._flog )
            if self._bit_array[digest]:

                continue

            self._bit_array[digest] = True
            _added = True

        return _added


    def _check(self, item):

        """
        Check the existence of an item in the filter
        :param item:
        :return:
        """

        for i in range(self._hash_count):
            digest = mmh3.hash(item, i) % self._size
            if not self._bit_array[digest]:
                # if any of bit is False then, its not present in the filter.
                # else there is probability that it exists

                return False
        return True

    def _filter_array_for_storing(self):
        """
        creates bytes string for storing in DB
        :return: size of bit array, number of hash function, bytestring for filter bit array
        """


        return self._size, self._hash_count, bitarray2string( self._bit_array )   # self._bit_array.tobytes()

    @staticmethod
    def _filter_array_from_storing(size_of_b_a, hash_count, str_bitarray) :  #b_a_bytes):

        """
        create filter bit array from bytes string
        :type b_a_bytes: object
        :param size_of_b_a:
        :param hash_count:
        :param b_a_bytes:
        :return: size of filter bit array, number of hash functions, filter bit array
        """

        print("size_of_b_a={}, hash_count={}, compacted string of bit array = \n{}".format(size_of_b_a, hash_count, str_bitarray))  #b_a_bytes))

        b_a = string2bitarray( str_bitarray )
        #b_a = bitarray(0)
        #b_a.frombytes( str(b_a_bytes).encode() )
        return size_of_b_a, hash_count, b_a

    @classmethod
    def _get_size(cls, n, p):

        """
        Return the size of bit arrray(m) to using following formula:

        m = - (n * ln(p))/(ln(2)^2)

        :param n: int
            number of items expected to be stored in the filter
        :param p: float
            False Positive probability in decimal
        :return: rounded up divide 8
        """
        print(type(n),type(p))
        k = - float( (float(n) * math.log(p)) )/ float( (math.log(float(2)) ** 2) )
        l = int(k)  # type: int
        if l % 8 != 0:
            l = int((l / 8) + 1) * 8

        return l

    @classmethod
    def _get_hash_count(cls, m, n):
        """
        Return the hash function(k) to be used using following formula
        k = (m/n) * math.log(2)
        :param m: int
            size of bit array
        :param n: int
            number of items expected to be stored in filter
        :return:
        """

        k = float((m / n)) * math.log(float(2))

        return int(k)


if __name__ == "__main__":
    pass
