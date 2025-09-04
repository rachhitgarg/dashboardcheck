import pandas as pd
import os

def convert_excel_to_csv():
    # List of Excel files to convert
    excel_files = [
        'AI Tutor Usage Summary Template.xlsx',
        'AI_for TKT exam Template.xlsx',
        'AI_Mentor_Feedback_Template.xlsx',
        'AI-initiatives impact.xlsx',
        'JPT_Usage Analysis and Placement tracker (1).xlsx',
        'unit-wise performance analysis template.xlsx'
    ]
    
    for file in excel_files:
        if os.path.exists(file):
            try:
                # Read Excel file
                df = pd.read_excel(file)
                
                # Convert to CSV
                csv_filename = file.replace('.xlsx', '.csv')
                df.to_csv(csv_filename, index=False)
                print(f"Converted {file} to {csv_filename}")
                
                # Display basic info
                print(f"Shape: {df.shape}")
                print(f"Columns: {list(df.columns)}")
                print(f"First few rows:")
                print(df.head())
                print("-" * 50)
                
            except Exception as e:
                print(f"Error converting {file}: {e}")
        else:
            print(f"File {file} not found")

if __name__ == "__main__":
    convert_excel_to_csv()
