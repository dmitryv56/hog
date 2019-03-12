#!/usr/bin/python3

# Copyright (C) Mellanox Technologies, Ltd. 2019-2019. ALL RIGHTS RESERVED.
#
# This software product is a proprietary product of Mellanox Technologies, Ltd.(the "Company") and all right, title, and
# interest in and to the software product, including all associated intellectual property rights, are and shall remain
# exclusively with the Company. All rights in or to the software product are licensed, not sold.
# All rights not licensed are reserved.
#
# This software product is governed by the End User License Agreement provided with the software product.
#


from random import shuffle

from bitarray import bitarray
from bloomfilter import BloomFilter

n = 20  # no of items to add
p = 0.05  # false positive probability

bloomf = BloomFilter(n, p, None)  # type: object

print("Size of bit array:{}".format(bloomf._size))
print("False positive Probability:{}".format(bloomf._fp_prob))
print("Number of hash functions:{}".format(bloomf._hash_count))

# words to be added
word_present = ['abound', 'abounds', 'abundance', 'abundant', 'accessable',
                'bloom', 'blossom', 'bolster', 'bonny', 'bonus', 'bonuses',
                'coherent', 'cohesive', 'colorful', 'comely', 'comfort',
                'gems', 'generosity', 'generous', 'generously', 'genial']

# word not added
word_absent = ['bluff', 'cheater', 'hate', 'war', 'humanity',
               'racism', 'hurt', 'nuke', 'gloomy', 'facebook',
               'geeksforgeeks', 'twitter']

for item in word_present:
    bloomf._add(item)

_size, _hash_count, _bytestring = bloomf._filter_array_for_storing()

#####################################
b_a = bitarray(0)
b_a.frombytes(_bytestring)

shuffle(word_present)
shuffle(word_absent)

test_words = word_present[:10] + word_absent
shuffle(test_words)
for word in test_words:
    if bloomf._check(word):
        if word in word_absent:
            print("'{}' is a false positive!".format(word))
        else:
            print("'{}' is probably present!".format(word))
    else:
        print("'{}' is definitely not present!".format(word))

size_, hash_count_, b_a_ = BloomFilter._filter_array_from_storing(_size, _hash_count, _bytestring)
bloomf1 = BloomFilter(n, p, None, b_a, b_a.length())

pass
