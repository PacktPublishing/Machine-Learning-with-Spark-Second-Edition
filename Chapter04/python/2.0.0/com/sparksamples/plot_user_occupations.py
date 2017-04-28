import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from util import get_user_data


def main():
    user_data = get_user_data()
    user_occ = user_data.groupby("occupation").count().collect()

    user_occ_len = len(user_occ)
    user_occ_list = []
    for i in range(0, (user_occ_len - 1)):
        element = user_occ[i]
        count = element. __getattr__('count')

        tup = (element.occupation, count)
        user_occ_list.append(tup)
    x_axis1 = np.array([c[0] for c in user_occ_list])
    y_axis1 = np.array([c[1] for c in user_occ_list])
    x_axis = x_axis1[np.argsort(y_axis1)]
    y_axis = y_axis1[np.argsort(y_axis1)]

    pos = np.arange(len(x_axis))
    width = 1.0

    ax = plt.axes()
    ax.set_xticks(pos + (width / 2))
    ax.set_xticklabels(x_axis)

    plt.bar(pos, y_axis, width, color='lightblue')
    plt.xticks(rotation=45, fontsize='9')
    plt.gcf().subplots_adjust(bottom=0.15)
    #fig = matplotlib.pyplot.gcf()


    plt.show()



if __name__ == "__main__":
    main()