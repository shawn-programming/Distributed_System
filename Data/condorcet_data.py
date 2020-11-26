import pandas as pd
import numpy as np
import random




def condorcet_data(candidate_num, iteration):
    data_frame = {'first':None, 'second':None, 'third':None}

    first,second,third =list(),list(),list()

    #candidate_A, candidate_B, winner = list(), list(), list()


    for i in range(iteration):
        for j in range(candidate_num):
            for k in range(j+1, candidate_num):

                r = random.randint(0,5)


                num = 0
                if r == 0:
                    num=123
                elif r==1:
                    num=132
                elif r==2:
                    num=213
                elif r==3:
                    num=231
                elif r==4:
                    num=312
                elif r==5:
                    num=321

                first.append(int(num/100))
                num = num % 100
                second.append(int(num/10))
                num = num % 10
                third.append(num)

    data_frame["first"] = first
    data_frame["second"] = second
    data_frame["third"] = third

    pandas_df = pd.DataFrame(data_frame)

    print(pandas_df.head())

    pandas_df.to_csv('condorcet_data.csv')


def contact_tracing_db(name_num, location_change_freq):
    locations = ["Davenport Hall", "McKinley Health Center", "Foellinger Auditorium", "Chemistry Annex", "Ice Arena", "	Engineering Hall", \
        "Illini Union", "Altgeld Hall", "Lincoln Hall", "Observatory", "Music Building", "Gregory Hall", "English Building", "Krannert Center", \
        "Illini Hall", "Mumford Hall", "State Farm Center", "Japan House", "ECE Building", "Siebel Center"]

    name_db = pd.read_csv("Common_Surnames_Census_2000.csv")
    names = list(name_db["name"])[:name_num]
    names = np.array(names)

    np.random.shuffle(names)
    day_in_seconds = 60 * 60 * 24

    personal_info_db = {"Unique_Person_Name": None, "location": None, "start_time": None, "end_time": None}

    infection_db = {"Unique_Person_Name": None, "Is_Infected": None}

    Unique_Person_Name, location, start_time, end_time, UPN_infected, Is_Infected = list(), list(), list(), list(), list(), list()


    for name in names:
        time_line = [0] + random.sample(range(day_in_seconds), location_change_freq-1) + [day_in_seconds]
        time_line.sort()
        places = list(np.random.randint(len(locations), size=location_change_freq))

        for i in range(location_change_freq):
            temp_Unique_Person_Name = name
            temp_location = locations[places[i]]
            temp_start_time = time_line[i]
            temp_end_time = time_line[i+1]
            temp_Is_Infected = True if random.randint(0, 1) > 0 else False

            Unique_Person_Name.append(temp_Unique_Person_Name)
            location.append(temp_location)
            start_time.append(temp_start_time)
            end_time.append(temp_end_time)
        UPN_infected.append(name)
        Is_Infected.append(temp_Is_Infected)

    personal_info_db["Unique_Person_Name"] = Unique_Person_Name
    personal_info_db["location"] = location
    personal_info_db["start_time"] = start_time
    personal_info_db["end_time"] = end_time

    infection_db["Unique_Person_Name"] = UPN_infected
    infection_db["Is_Infected"] = Is_Infected

    personal_info_df = pd.DataFrame(personal_info_db)
    infection_df = pd.DataFrame(infection_db)

    print(personal_info_df.head())
    print(infection_df.head())
    personal_info_df.to_csv('personal_info_df.csv')
    infection_df.to_csv('infection_df.csv')







def main():
    # print("Creatinig Condorcet DataFrame...")

    # condorcet_data(35, 400)

    # print("Condorcet DataFrame created.\n")

    print("Creatinig Contact Tracing DataFrame...")

    contact_tracing_db(3000, 60)

    print("Contact Tracing DataFrame created.")


if __name__ == "__main__":
    main()
