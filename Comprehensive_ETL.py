import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Define the path to the directory containing  data
data_directory = r"E:\\Guvi Project\\Comperhensive_ELT\\source\\"

# Log file path
log_file = "E:\\Guvi Project\\Comperhensive_ELT\\source\\log_file.txt"

# Final output path for transformed data
transformed_data_path = "E:\\Guvi Project\\Comperhensive_ELT\\source\\transformed_data.csv"

# Example paths for different file types...it will get list of desired files name
csv_files = glob.glob(data_directory + "*.csv")
json_files = glob.glob(data_directory + "*.json")
xml_files = glob.glob(data_directory + "*.xml")


def log_progress(message):
    try:
        with open(log_file, "a") as f:
            f.write(f"{datetime.now()}: {message}\n")
    except Exception as e:
        print(f"Failed to log message: {e}")  # Print or handle the error

# Function to extract data from CSV files
def extract_csv(file_path):
    return pd.read_csv(file_path)

# Function to extract data from JSON files
def extract_json(file_path):
    return pd.read_json(file_path,lines=True)


# Function to extract data from XML files

data=[]
def extract_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    for i in root.findall("person"):
        Name=i.find("name").text
        Height=i.find("height").text
        Weigth=i.find("weight").text
        data.append({"Name":Name,"Height":Height,"Weigth":Weigth})

    df=pd.DataFrame(data)

#A master function will call the relevant function 

def extract_data():
    combined_data = pd.DataFrame()
    
    # Extracting CSV data
    for file in csv_files:
        csv_data = extract_csv(file)
        combined_data = pd.concat([combined_data, csv_data], ignore_index=True)
        log_progress(f"Extracted data from CSV: {file}")

    # Extracting JSON data
    for file in json_files:
        json_data = extract_json(file)
        combined_data = pd.concat([combined_data, json_data], ignore_index=True)
        log_progress(f"Extracted data from JSON: {file}")

    # Extracting XML data
    for file in xml_files:
        xml_data = extract_xml(file)
        combined_data = pd.concat([combined_data, xml_data], ignore_index=True)
        log_progress(f"Extracted data from XML: {file}")

    log_progress("Data extraction completed.")
    return combined_data

extracted_data = extract_data()  # Call to extract data


#Transform Data:
extracted_data['height'] = extracted_data['height'].astype(float) * 0.0254
extracted_data['height'] = extracted_data['height'].astype(float) * 0.453592

extracted_data.to_csv(transformed_data_path,index=False)
log_progress("Transform Data completed.")

print(extracted_data.head())
