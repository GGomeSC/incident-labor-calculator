import pandas as pd
import os
import re
import json
import time

def load_config(): # Create a config.json file in the same directory as this script and speficy the root_folder's path and output_directory'path that you want
    # Specify the full path to the config file
    dir_path = os.path.dirname(os.path.realpath(__file__))  # Gets the directory where the script is located
    config_path = os.path.join(dir_path, 'config.json')  # Path to the config file
    print("Current working directory:", os.getcwd())
    with open(config_path, 'r') as config_file:
        return json.load(config_file)

def parse_duration(duration_str):
    total_minutes = 0
    if pd.isna(duration_str):
        return 0
    hours = re.findall(r'(\d+)\s*h', duration_str)
    minutes = re.findall(r'(\d+)\s*min', duration_str)
    seconds = re.findall(r'(\d+)\s*s', duration_str)
    if hours:
        total_minutes += int(hours[0]) * 60
    if minutes:
        total_minutes += int(minutes[0])
    if seconds:
        total_minutes += int(seconds[0]) // 60 if int(seconds[0]) >= 30 else 0
    return total_minutes

def minutes_to_hours(minutes):
    hours = minutes // 60
    remainder_minutes = minutes % 60
    print("Converting minutes to hours...")
    return f"{hours}:{remainder_minutes:02d}"

def aggregate_excel_files(root_folder):
    aggregated_df = pd.DataFrame()
    for subdir, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith('.xlsx'):
                file_path = os.path.join(subdir, file)
                df = pd.read_excel(file_path)
                df['Incident Info'] = os.path.basename(subdir)  # Assign file name to Incident Info
                aggregated_df = pd.concat([aggregated_df, df], ignore_index=True)
                print("Aggregating all in one Excel file...")
    return aggregated_df

def main():
    start_time = time.time()  # Record the start time
    config = load_config()
    root_folder = config['root_folder']
    # root_folder = r'path_to_Attendance_Reports_folder'  # Replace with your folder path
    output_directory = config['output_directory']
    # output_directory = r'path_to_Output_folder'  # Define the output directory
    output_file = 'total_duration_per_participant_per_incident.xlsx'

    df = aggregate_excel_files(root_folder)
    df['Incident ID'] = df['Incident Info'].str.extract(r'(GV-\d+)') # Regex that extracts the incident ID from that meeting title, in our case it's "GV-"
    df['Incident ID'] = df['Incident ID'].fillna(df['Incident Info'])
    df['Duration in minutes'] = df['Duração'].apply(parse_duration)
    aggregated_df = df.groupby(['Incident ID', 'Enviar e-mail']).agg({'Duration in minutes': 'sum'}).reset_index()
    aggregated_df['Meetings Attended'] = df.groupby(['Incident ID', 'Enviar e-mail'])['Incident Info'].transform('nunique')
    aggregated_df['Formatted Duration'] = aggregated_df['Duration in minutes'].apply(minutes_to_hours)

    # Ensure the output directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print("Creating the output directory...")
    
    # Save the file to the specified directory
    aggregated_df.to_excel(os.path.join(output_directory, output_file), index=False)
    print(f"Data has been aggregated and saved to '{os.path.join(output_directory, output_file)}'.")
    
    end_time = time.time()  # Record the end time
    execution_time = end_time - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == '__main__':
    main()