import os
import sys
from collections import defaultdict

def search_files(directory, search_string):
    """
    Search all files in the given directory for the specified string.

    Args:
        directory (str): The directory to search.
        search_string (str): The string to search for.

    Returns:
        dict: A dictionary where the keys are file paths and the values are
        lists of tuples, each containing the line number and line content
        where the search string was found.
    """
    results = defaultdict(list)
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                with open(file_path, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        if search_string in line:
                            results[file_path].append((line_num, line.strip()))
            except UnicodeDecodeError:
                print(f"Unable to read file {file_path}, it may be a non-text file.")
    return results

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <directory> <search_string>")
        sys.exit(1)

    directory, search_string = sys.argv[1], sys.argv[2]
    results = search_files(directory, search_string)

    if results:
        print(f"Found the search string '{search_string}' in the following files:")
        for file_path, matches in results.items():
            print(f"- {file_path}")
            for line_num, line in matches:
                print(f"  Line {line_num}: {line}")
    else:
        print(f"No matches for '{search_string}' found in directory '{directory}'.")
