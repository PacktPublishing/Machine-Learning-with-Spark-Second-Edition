import numpy as np
from numpy import cumsum
import matplotlib.pyplot as plt

from PIL import Image, ImageFilter
import os

PATH = "../../../../data"


def main():

    path = PATH + "/lfw/Aaron_Eckhart/Aaron_Eckhart_0001.jpg"

    im = Image.open(path)
    im.show()

    tmp_path = PATH + "/aeGray.jpg"
    ae_gary = Image.open(tmp_path)
    ae_gary.show()

    pc = np.loadtxt(PATH + "/pc.csv", delimiter=",")
    print(pc.shape)
    # (2500, 10)


    plot_gallery(pc, 50, 50)


def plot_gallery(images, h, w, n_row=2, n_col=5):
        """Helper function to plot a gallery of portraits"""
        plt.figure(figsize=(1.8 * n_col, 2.4 * n_row))
        plt.subplots_adjust(bottom=0, left=.01, right=.99, top=.90, hspace=.35)
        for i in range(n_row * n_col):
            plt.subplot(n_row, n_col, i + 1)
            plt.imshow(images[:, i].reshape((h, w)), cmap=plt.cm.gray)
            plt.title("Eigenface %d" % (i + 1), size=12)
            plt.xticks(())
            plt.yticks(())
            plt.show()

if __name__ == "__main__":
    main()