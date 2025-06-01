import json
import re
import os

# Load the JSON data
with open('tech_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Function to sanitize filenames
def sanitize_filename(filename):
    # Remove invalid characters for filenames
    sanitized = re.sub(r'[\\/*?:"<>|]', '', filename)
    # Limit the length of the filename
    return sanitized[:50]

# Create the output directory if it doesn't exist
output_dir = 'output'
os.makedirs(output_dir, exist_ok=True)

# Loop through each item in the JSON data
for item in data:
    # Sanitize the title to use as the filename
    title = sanitize_filename(item['title'])
    filename = os.path.join(output_dir, f"{title}.txt")

    # Open a text file to write the extracted data
    with open(filename, 'w', encoding='utf-8') as txt_file:
        # Write the title
        txt_file.write(f"Title: {item['title']}\n\n")

        # Write the body
        txt_file.write(f"Body: {item['body']}\n\n")

        # Write the comments
        txt_file.write("Comments:\n")
        for comment in item['comments']:
            txt_file.write(f"  - {comment['body']}\n")

print("Threads have been extracted and saved to separate text files in the 'output' directory.")
