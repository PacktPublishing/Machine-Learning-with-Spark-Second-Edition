from util import get_user_data


def main():
    user_data = get_user_data()
    num_users = user_data.count()
    num_genders = len(user_data.groupBy("gender").count().collect())
    num_occupation = len(user_data.groupBy("occupation").count().collect())
    num_zipcodes = len(user_data.groupby("zipCode").count().collect())
    print("Users: " + str(num_users))
    print("Genders: " + str(num_genders))
    print("Occupation: " + str(num_occupation))
    print("ZipCodes: " + str(num_zipcodes))


if __name__ == "__main__":
    main()