import os
import pandas as pd

def split_csv(input_filepath, output_folder, rows_per_chunk):
    try:
        # Create the output folder if it doesn't exist
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Read the CSV file
        df = pd.read_csv(input_filepath)

        # Calculate the number of chunks needed
        num_rows = len(df)
        num_chunks = (num_rows + rows_per_chunk - 1) // rows_per_chunk

        # Split and save the chunks
        for chunk_index in range(num_chunks):
            start_row = chunk_index * rows_per_chunk
            end_row = min((chunk_index + 1) * rows_per_chunk, num_rows)
            
            chunk_df = df[start_row:end_row]
            output_filepath = os.path.join(output_folder, f"chunk_{chunk_index + 1}.csv")
            chunk_df.to_csv(output_filepath, index=False)
            print(f"Chunk {chunk_index + 1}/{num_chunks} saved to {output_filepath}")

    except PermissionError:
        print("Permission denied. Make sure you have write permissions to the output folder.")

if __name__ == "__main__":
    input_filepath = "C:/Users/Dell XPS/Documents/python-tools/csvSplitter/csv/300k-dada.csv"
    output_folder = "C:/Users/Dell XPS/Documents/python-tools/csvSplitter/output"
    rows_per_chunk = 10000  # Adjust this value as needed

    split_csv(input_filepath, output_folder, rows_per_chunk)



    # input_filepath = r"C:/Users/Dell XPS/Documents/python-tools/csvSplitter/csv"
    # output_folder = r"C:/Users/Dell XPS/Documents/python-tools/csvSplitter/output"