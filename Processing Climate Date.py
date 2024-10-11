import os
import pandas as pd

# Directory containing the .dly files
input_folder = r'C:\Users\sarab\Desktop\desktop\Valley Fever\Data\ghcnd_hcn\ghcnd_hcn'
output_folder = os.path.join(os.path.dirname(input_folder), 'converted_csv_files')

# Create the output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Define column widths based on the provided guide
colspecs = [(0, 11), (11, 15), (15, 17), (17, 21)]
for day in range(1, 32):
    colspecs.extend([(21 + (day - 1) * 8, 26 + (day - 1) * 8),
                     (26 + (day - 1) * 8, 27 + (day - 1) * 8),
                     (27 + (day - 1) * 8, 28 + (day - 1) * 8),
                     (28 + (day - 1) * 8, 29 + (day - 1) * 8)])

# Load station data
station_file = 'C:/Users/sarab/Desktop/desktop/Valley Fever/Data/ghcnd-stations.txt'
stations_data = []
with open(station_file, 'r') as file:
    for line in file:
        parts = line.split()
        if len(parts) >= 6:
            station_id = parts[0]
            latitude = float(parts[1])
            longitude = float(parts[2])
            elevation = float(parts[3])
            state = parts[4] if len(parts[4]) == 2 else None
            name = ' '.join(parts[5:])
            stations_data.append([station_id, latitude, longitude, elevation, state, name])

stations_df = pd.DataFrame(stations_data, columns=['ID', 'Latitude', 'Longitude', 'Elevation', 'State', 'Name'])

# Define county-specific keywords
county_keywords_dicts = {
    "Maricopa": ['MARICOPA', 'PHOENIX', 'TEMPE', 'MESA', 'SCOTTSDALE', 'GLENDALE', 'CHANDLER', 'GILBERT',
                 "PEORIA", "SURPRISE", "AVONDALE", "GOODYEAR", "BUCKEYE", "EL MIRAGE", "FOUNTAIN HILLS",
                 "PARADISE VALLEY", "TOLLESON"],
    "Pinal": ["CASA GRANDE", "FLORENCE", "APACHE JUNCTION", "COOLIDGE", "ELOY", 'MARICOPA'],
    "Pima": ['TUCSON', "ORO VALLEY", "SAHUARITA", 'MARANA', 'SOUTH TUCSON']
}

def contains_county(name_parts, counties):
    return any(county in name_parts for county in counties)

for county_name, cities_identifiers in county_keywords_dicts.items():
    stations_df_filtered = stations_df[(stations_df['State'] == 'AZ')].copy()

    stations_df_filtered['County'] = stations_df_filtered['Name'].apply(
        lambda x: contains_county(x.upper().split(), cities_identifiers))

    stations_of_interest = stations_df_filtered[stations_df_filtered['County']]['ID'].tolist()
    print(f"\nProcessing County: {county_name}")
    print(f"Found {len(stations_of_interest)} stations")

    # Process only the selected stations' .dly files
    for filename in os.listdir(input_folder):
        if filename.endswith('.dly'):
            station_id = filename.split('.')[0]
            if station_id not in stations_of_interest:
                continue  # Skip files not in the selected stations list

            file_path = os.path.join(input_folder, filename)
            df = pd.read_fwf(file_path, colspecs=colspecs, header=None)

            # Assign names to the columns
            columns = ['ID', 'YEAR', 'MONTH', 'ELEMENT']
            for day in range(1, 32):
                columns.extend([f'VALUE{day}', f'MFLAG{day}', f'QFLAG{day}', f'SFLAG{day}'])
            df.columns = columns

            # Reshape the DataFrame to have each day as a separate row
            data_rows = []
            for _, row in df.iterrows():
                for day in range(1, 32):
                    value_col = f'VALUE{day}'
                    if pd.notnull(row[value_col]) and row[value_col] != -9999:  # Skip missing values (-9999)
                        data_rows.append({
                            'ID': row['ID'],
                            'YEAR': row['YEAR'],
                            'MONTH': row['MONTH'],
                            'DAY': day,
                            'ELEMENT': row['ELEMENT'],
                            'VALUE': row[f'VALUE{day}'],
                            'MFLAG': row[f'MFLAG{day}'],
                            'QFLAG': row[f'QFLAG{day}'],
                            'SFLAG': row[f'SFLAG{day}']
                        })

            # Convert the reshaped data into a DataFrame
            df_cleaned = pd.DataFrame(data_rows)

            # Save the reshaped data into a CSV file
            output_file = os.path.join(output_folder, f'{station_id}.csv')
            df_cleaned.to_csv(output_file, index=False)
            print(f'Converted {filename} to CSV format in {output_folder}.')
