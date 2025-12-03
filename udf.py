
# create a UDF (User defined function)
def age_grp(age):
    if age is None:
        # print("Unknown")
        return "Unknown"
    elif age <= 18:
        # print("Minor")
        return "Minor"
    elif age <= 40:
        # print("adult")
        return "Adult"
    elif age <= 60:
        # print("Senior")
        return "Senior"
    else:
        return "senior citizen"


age_grp(30)