import csv


def write_to_csv(data):
    file_path = "data/earnings_data.csv"
    with open(file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)