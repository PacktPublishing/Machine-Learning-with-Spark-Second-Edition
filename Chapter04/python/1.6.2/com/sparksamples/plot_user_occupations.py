import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from util import get_user_data



def main():
    user_data = get_user_data()
    user_fields = user_data.map(lambda line: line.split("|"))
    count_by_occupation = user_fields.map(lambda fields: (fields[3], 1)).reduceByKey(lambda x, y: x + y).collect()
    x_axis1 = np.array([c[0] for c in count_by_occupation])
    y_axis1 = np.array([c[1] for c in count_by_occupation])
    x_axis = x_axis1[np.argsort(y_axis1)]
    y_axis = y_axis1[np.argsort(y_axis1)]

    pos = np.arange(len(x_axis))
    width = 1.0

    ax = plt.axes()
    ax.set_xticks(pos + (width / 2))
    ax.set_xticklabels(x_axis)

    plt.bar(pos, y_axis, width, color='lightblue')
    plt.xticks(rotation=30)
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(20, 10)
    plt.show()



if __name__ == "__main__":
    main()