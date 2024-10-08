import os
import pandas as pd

# Directory containing the .dly files
input_folder = r'C:\Users\sarab\Desktop\desktop\Valley Fever\Data\ghcnd_hcn\ghcnd_hcn'

# Create a new folder for CSV files beside the input folder
output_folder = os.path.join(os.path.dirname(input_folder), 'converted_csv_files')

# Create the output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Define column widths based on the provided guide
colspecs = [
    (0, 11),  # ID (1-11)
    (11, 15),  # YEAR (12-15)
    (15, 17),  # MONTH (16-17)
    (17, 21),  # ELEMENT (18-21)
]

# Add specs for the 31 days in the month (value, mflag, qflag, sflag for each day)
for day in range(1, 32):
    colspecs.extend([
        (21 + (day - 1) * 8, 26 + (day - 1) * 8),  # VALUE
        (26 + (day - 1) * 8, 27 + (day - 1) * 8),  # MFLAG
        (27 + (day - 1) * 8, 28 + (day - 1) * 8),  # QFLAG
        (28 + (day - 1) * 8, 29 + (day - 1) * 8),  # SFLAG
    ])

# Process each .dly file in the input folder
for filename in os.listdir(input_folder):
    if filename.endswith('.dly'):
        file_path = os.path.join(input_folder, filename)

        # Read the .dly file
        df = pd.read_fwf(file_path, colspecs=colspecs, header=None)

        # Assign names to the columns
        columns = ['ID', 'YEAR', 'MONTH', 'ELEMENT']
        for day in range(1, 32):
            columns.extend([
                f'VALUE{day}', f'MFLAG{day}', f'QFLAG{day}', f'SFLAG{day}'
            ])
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
        output_file = os.path.join(output_folder, f'{os.path.splitext(filename)[0]}.csv')
        df_cleaned.to_csv(output_file, index=False)

        print(f'Converted {filename} to CSV format in {output_folder}.')

