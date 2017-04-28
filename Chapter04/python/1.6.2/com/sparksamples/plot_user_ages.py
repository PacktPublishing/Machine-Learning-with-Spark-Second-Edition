
import matplotlib
import matplotlib.pyplot as plt
from util import get_user_data


def main():
    user_data = get_user_data()
    user_fields = user_data.map(lambda line: line.split("|"))
    ages = user_fields.map(lambda x: int(x[1])).collect()
    plt.hist(ages, bins=20, color='lightblue', normed=True)
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(16, 10)
    plt.show()


if __name__ == "__main__":
    main()