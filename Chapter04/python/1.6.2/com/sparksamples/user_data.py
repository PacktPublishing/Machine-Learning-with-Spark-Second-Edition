from util import get_user_data


def main():
    user_data = get_user_data()
    user_data.first()

    user_fields = user_data.map(lambda line: line.split("|"))
    num_users = user_fields.map(lambda fields: fields[0]).count()
    num_genders = user_fields.map(lambda fields: fields[2]).distinct().count()
    num_occupations = user_fields.map(lambda fields: fields[3]).distinct().count()
    num_zipcodes = user_fields.map(lambda fields: fields[4]).distinct().count()
    print "Users: %d, genders: %d, occupations: %d, ZIP codes: %d" % \
          (num_users, num_genders, num_occupations, num_zipcodes)


if __name__ == "__main__":
    main()