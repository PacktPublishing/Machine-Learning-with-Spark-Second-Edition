import numpy as np
from numpy import cumsum
import matplotlib.pyplot as plt

PATH = "../../../data"


def main():
    file_name = '/home/ubuntu/work/ml-resources/spark-ml/Chapter_09/data/s.csv'
    data = np.genfromtxt(file_name, delimiter=',')

    plt.plot(data)
    plt.suptitle('Variation 300 Singular Values ')
    plt.xlabel('Singular Value No')
    plt.ylabel('Variation')
    plt.show()

    plt.plot(cumsum(data))
    plt.yscale('log')
    plt.suptitle('Cumulative Variation 300 Singular Values ')
    plt.xlabel('Singular Value No')
    plt.ylabel('Cumulative Variation')
    plt.show()



if __name__ == "__main__":
    main()