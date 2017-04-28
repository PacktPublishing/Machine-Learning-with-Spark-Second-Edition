
import matplotlib
import matplotlib.pyplot as plt
from util import get_user_data


def main():
    user_data = get_user_data()
    user_ages = user_data.select('age').collect()
    user_ages_list = []
    user_ages_len = len(user_ages)
    for i in range(0, (user_ages_len - 1)):
        user_ages_list.append(user_ages[i].age)
    plt.hist(user_ages_list, bins=20, color='lightblue', normed=True)
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(16, 10)
    plt.show()


if __name__ == "__main__":
    main()