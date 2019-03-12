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


from os import listdir
from os.path import isfile, join, isdir

import numpy as np
from scipy.misc import imread, imresize

DBG_PRINT = 0


class hog(object):
    """This simple class imlements the histogram of oriented gradients algorithm of image



    """
    HIST_BINS = 9
    HIST_IN_BLOCK = 4
    CELL_SIZE = 8
    QUANT_LEVELS = 256

    def __init__(self, path2img, N, M, channel=0):
        """

        :rtype: object
        """
        self._img = None
        self._path2img = path2img
        self._shape_img = None
        self._shape = None
        self._type_img = None
        self._gx = None
        self._gy = None
        self._channel = channel
        self._num_of_channels = 3  # 3 channels , for example RGB, 1 -greyscale (black -white)
        self._resizeX = N
        self._resizeY = M
        self._images = {}  # 0:<array>, 1:<array> , 2:<array>
        self._feature_vect = {}  # 0:<array>, 1:<array>, 2:<array>
        self._len_fvect = None
        self._gx = None
        self._gy = None
        self._magnitude = None
        self._direction = None
        self._final_fvect = None
        self._hash32 = None # np.zeros(1, dtype = np.uint32 )
        self._hexstr_feature = ""

    def _feature_vector_size(self):
        """

        :return:
        """
        if self._resizeX % self.CELL_SIZE != 0:
            self._resizeX = int(self._resizeX / self.CELL_SIZE) * self.CELL_SIZE
        if self._resizeY % self.CELL_SIZE != 0:
            self._resizeY = int(self._resizeY / self.CELL_SIZE) * self.CELL_SIZE

        self._len_fvect = (int(self._resizeX / self.CELL_SIZE) - 1) * (int(self._resizeY / self.CELL_SIZE) - 1) * int(
            self.HIST_BINS) * int(self.HIST_IN_BLOCK)

    def _read_image(self):
        """

        :param self:
        :return:
        """

        self._img = imread(self._path2img)
        self._num_of_channels = 3 if 3 == len(self._img.shape) else 1  # 3-rgb, 1 -greyscale image
        self._shape_img = self._img.shape
        self._type_img = self._img.dtype

    def _resize(self):
        """
        Checks the resized image has a multiple of 8 axis sizes
        sets the length of the feature vectors
        resizes the image per channels and creates dictionary of black-white images

        :return:
        """

        self._feature_vector_size()
        self._shape = (self._resizeX, self._resizeY)

        if self._num_of_channels < 3:  # grey (black-white) image, only 1 channel
            self._images[0] = imresize(self._img, (self._resizeX, self._resizeY))
            self._feature_vect[0] = np.zeros(self._len_fvect)

        else:  # 3 channels

            for i in range(self._img.shape[2]):
                self._images[i] = imresize(self._img[:, :, i], (self._resizeX, self._resizeY))
                self._feature_vect[i] = np.zeros(self._len_fvect)

        self._gx = np.zeros(shape=self._shape, dtype=int)
        self._gy = np.zeros(shape=self._shape, dtype=int)
        self._magnitude = np.zeros(shape=self._shape)
        self._direction = np.zeros(shape=self._shape)

    # def grad_images(img, gx, gy):
    def _grad_images(self, channel):
        """

        :param channel:
        :return:
        """

        img = self._images[channel]
        for i in range(img.shape[0]):
            for j in range(img.shape[1]):

                if i == 0:
                    self._gy[i, j] = img[i + 1, j] - 0
                elif i == img.shape[0] - 1:
                    self._gy[i, j] = 0 - img[i - 1, j]
                else:
                    self._gy[i, j] = int(img[i + 1, j]) - int(img[i - 1, j])

                if j == 0:
                    self._gx[i, j] = img[i, j + 1] - 0
                elif j == img.shape[1] - 1:
                    self._gx[i, j] = 0 - img[i, j - 1]
                else:

                    self._gx[i, j] = int(img[i, j + 1]) - int(img[i, j - 1])

    def _grad(self):

        # for i in range (gx.shape[0]):
        #     for j in range (gx.shape[1]):
        #         Gx=gx[i,j]
        #         Gy=gy[i,j]
        #
        #         gx[i,j]=np.sqrt( Gx*Gx + Gy*Gy )
        #         gy[i,j]=np.arctan2(Gy,Gx)*180/np.pi

        self._magnitude = np.sqrt(self._gx * self._gx + self._gy * self._gy)
        self._direction = np.rad2deg(np.arctan2(self._gy, self._gx))

        return

    def _fvect(self, channel):
        """
        It calculates  the Histogram of the Gradient (HOG) over 8x8 cell and normalizes it over 16x16 blocks. The
        final the feature vector for the entire image is calculated by moving 16x16 blocks in steps 8 (50% overlap
        with prvious block) and the 36 numbers (corresponding to 4 histograms in a 16x16 block) calculated at each
        step are concatenated to produce the final feature vector. The size of  vector is calculated as followed: The
        input image is N * M pixels in size, where N =k*8, M=l*8, and we are moving 8 pixels at a  time. Therefore,
        we can make (k-1) steps in the horizontal direction and (l-1) steps in vertical direction which adds up (k-1)
        * (l-1) steps. At each step we calculated 36 numbers ( 4 concatenated histograms) and so the length of the
        final feature vector is (k-1)*(l-1)*36 For example, for 64*128 pixel image the final feature vecto has size
        7*15*36=3780.


        :return:
        """

        # magnitude_matrix.shape(0),magnitude_matrix.shape(0) should be divided by 16
        block16 = 16
        cells8 = 8
        row_block = 0
        # the feature vector has a size (4*9) * shift_row * shift_column
        # feature_vector = init_fvect(magnitude_matrix)
        feature_vector = self._feature_vect[channel]

        bin_base = 0
        bins_in_hist = 9

        # 16x16 blocks
        # these blocks shifts by row and by column

        if DBG_PRINT:
            print(self._magnitude.shape[0], self._magnitude.shape[1])

        while row_block < self._magnitude.shape[0] - cells8:
            col_block = 0
            while col_block < self._magnitude.shape[1] - cells8:
                # print(self._magnitude.shape[0], self._magnitude.shape[1], row_block,col_block)

                block_magnitude = self._magnitude[row_block: row_block + block16, col_block: col_block + block16]
                block_direction = self._direction[row_block: row_block + block16, col_block: col_block + block16]

                xcell = 0
                offset_in_fvect = bin_base

                # 8x8 subblock of the cells are selected into 16x16 blocks
                # The 9-bin histogram  is calculated for each 8x8  subblock of cells.
                # The histogram bins correspond to gradien directions 0, 20, 40,...,160 degrees.

                while xcell < block_magnitude.shape[0]:
                    ycell = 0
                    while ycell < block_magnitude.shape[1]:
                        if DBG_PRINT:
                            print(self._magnitude.shape[0], self._magnitude.shape[1], row_block, col_block, xcell,
                                  ycell)

                        cells_magnitude = block_magnitude[xcell: xcell + cells8, ycell: ycell + cells8]
                        cells_direction = block_direction[xcell: xcell + cells8, ycell: ycell + cells8]

                        vect_magnitude = np.reshape(cells_magnitude,
                                                    (cells_magnitude.shape[0] * cells_magnitude.shape[1]))
                        vect_direction = np.reshape(cells_direction,
                                                    (cells_direction.shape[0] * cells_direction.shape[1]))
                        # print for debug
                        if bin_base > self._len_fvect - 40:
                            if DBG_PRINT:
                                print(bin_base, row_block, col_block, xcell, ycell)

                        for i in range(vect_magnitude.shape[0]):
                            vote_bin, vote_next_bin, bin_hist = self.vote(vect_magnitude[i], vect_direction[i])

                            feature_vector[bin_base + bin_hist] += vote_bin
                            if bin_hist < 8:  # to prevent write after last bin
                                feature_vector[bin_base + bin_hist + 1] += vote_next_bin

                        ycell += cells8
                        bin_base += bins_in_hist

                    xcell += cells8
                # normalization
                try:
                    norm4block = 0.0
                    for i in range(offset_in_fvect, offset_in_fvect + 36):
                        norm4block += feature_vector[i] * feature_vector[i]

                    norm4block = np.sqrt(norm4block)
                    for i in range(offset_in_fvect, offset_in_fvect + 36):
                        feature_vector[i] = feature_vector[i] / norm4block
                except Exception as e:
                    print("Exception in _fvect {}".format(e))
                finally:
                    pass
                col_block += cells8
            row_block += cells8

        return feature_vector

    def _final_feature_vector(self):
        if self._num_of_channels == 1:
            self._final_fvect = np.copy(self._feature_vect[0])
        elif self._num_of_channels == 3:
            self._final_fvect = self._feature_vect[0] + self._feature_vect[1] + self._feature_vect[2]
            self._final_fvect = self._final_fvect / 3

        else:
            pass

        ###############################################
        # scaling from (0 -:- 1] to [0 -:- 1024]
        #
        # round(x[i] * 1024 )
        for i in range(self._len_fvect):
            self._final_fvect[i] = int(self._final_fvect[i] * self.QUANT_LEVELS)

        return


    ###################################################################################################################
    # static methods of the hog class
    ###################################################################################################################
    @staticmethod
    def vec2hexstr(bit_resolution, vect):
        """

        :param bit_resolution:
        :param vect:
        :return:
        """
        s = ""

        for i in range(vect.shape[0]):
            a = int(vect[i])
            if bit_resolution == 8:
                s = s + "%0.2X" % a
            elif bit_resolution == 16:
                s = s + "%0.4X" % a
            elif bit_resolution == 32:
                s = s + "%0.8X" % a
        return s

    # static methods of the hog class
    @staticmethod
    def hexstr2vec(bit_resolution, str_vect):

        if bit_resolution == 8:
            stride = 2
        elif bit_resolution == 16:
            stride = 4
        elif bit_resolution == 32:
            stride = 8
        else:
            stride = 8

        offset = 0
        lenstr = len(str_vect)
        if str_vect[0:2] == "0x" or str_vect[0:2] == "0X":
            offset += 2
            len_vector = int((lenstr - 2) / stride)
        else:
            len_vector = int(lenstr / stride)
        i = 0
        vect = np.zeros(shape=(len_vector))
        while offset < lenstr:
            s = str_vect[offset:stride]
            vect[i] = int(s, 16)
            i += 1
            offset += stride

        return vect

    @staticmethod
    def vote(magnitude_value, direction_value):
        """

        :param magnitude_value:  magnitude of the gradient
        :param direction_value:  angle of gradient in the degrees from 0 till 180
        :return: vote1 - vote for the histogram bin
                 vote2 - for next bin in the histogram
                 bin_hist - the bin in the histogram (0,...,8)
        """
        if 0 <= direction_value < 20:

            vote1 = magnitude_value
            vote2 = 0
            bin_hist = 0

        elif 20 <= direction_value < 40:

            vote1, vote2 = hog.split_for_vote(magnitude_value, direction_value)
            bin_hist = 1

        elif 40 <= direction_value < 60:

            vote1, vote2 = hog.split_for_vote(magnitude_value, direction_value)
            bin_hist = 2

        elif 60 <= direction_value < 80:

            vote1, vote2 = hog.split_for_vote(magnitude_value, direction_value)
            bin_hist = 3

        elif 80 <= direction_value < 100:

            vote1, vote2 = hog.split_for_vote(magnitude_value, direction_value)
            bin_hist = 4

        elif 100 <= direction_value < 120:
            vote1, vote2 = hog.split_for_vote(magnitude_value, direction_value)
            bin_hist = 5

        elif 120 <= direction_value < 140:

            vote1, vote2 = hog.split_for_vote(magnitude_value, direction_value)
            bin_hist = 6

        elif 140 <= direction_value < 160:

            vote1, vote2 = hog.split_for_vote(magnitude_value, direction_value)
            bin_hist = 7

        elif direction_value >= 160:

            vote1 = magnitude_value
            vote2 = 0
            bin_hist = 8

        else:
            vote1 = vote2 = bin_hist = 0

        return vote1, vote2, bin_hist

    @staticmethod
    def split_for_vote(magnitude_value, direction_value):
        """
        This method splits the vote for two conjected bins.
        :rtype: object
        :param magnitude_value: magnitude of gradient
        :param direction_value: angle of gradient
        :return:
        """

        res_value = int(direction_value % 20)

        x = (magnitude_value / (20 + res_value)) * res_value

        #  20+res_value/magnitude   =  res_value/ x
        #
        # x= (magnitude/(20+res_value))*res_value
        #   (round (magnitude -x), round(x))

        return round(magnitude_value - x), round(x)


######################################################################################################################
######################################################################################################################

def list_img_files(base_path):
    if not isdir(base_path):
        return []

    return [join(base_path, f) for f in listdir(base_path) if isfile(join(base_path, f))]


if __name__ == "__main__":

    # imG=hog("/home/osboxes/face1.png", 64, 128 )
    # imG=hog(path_base, 64, 128 )
    # imG._read_image()  # read image
    #
    # imG._resize()
    # if (imG._num_of_channels<3 ):
    #     imG._grad_images(0)
    #     imG._grad()
    #     imG._fvect(0)
    # else:
    #
    #     for iChannel in range( imG._img.shape[2]):
    #         imG._grad_images( iChannel )
    #         imG._grad()
    #         imG._fvect( iChannel )

    #     path_base = "/home/osboxes/PycharmProjects/hog_data/faces/train/face/"
    path_base = "/home/osboxes/PycharmProjects/hog_data/pedestrian/"

    train_faces = list_img_files(path_base)

    for fname in train_faces:
        imG = hog(fname, 64, 128)
        imG._read_image()  # read image

        imG._resize()
        if imG._num_of_channels < 3:
            imG._grad_images(0)
            imG._grad()
            imG._fvect(0)
            imG._final_feature_vector()
        else:

            for iChannel in range(imG._img.shape[2]):
                imG._grad_images(iChannel)
                imG._grad()
                imG._fvect(iChannel)

            imG._final_feature_vector()
        print("fname {} passed".format(fname))
        del imG

    pass
