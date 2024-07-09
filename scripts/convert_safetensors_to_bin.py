import torch
from safetensors.torch import load_file
import argparse

def convert_safetensors_to_bin(safetensors_path, bin_path):
    # Load the safetensors file
    state_dict = load_file(safetensors_path)

    # Save the state_dict to a .bin file
    torch.save(state_dict, bin_path)
    print(f"Converted {safetensors_path} to {bin_path}")

def main():
    # Set up the argument parser
    parser = argparse.ArgumentParser(description="Convert safetensors file to PyTorch bin file.")
    parser.add_argument("--safetensors_path", help="Path to the safetensors file")
    parser.add_argument("--bin_path", help="Path to save the PyTorch bin file")

    # Parse the command line arguments
    args = parser.parse_args()

    # Call the conversion function with the provided arguments
    convert_safetensors_to_bin(args.safetensors_path, args.bin_path)

if __name__ == "__main__":
    main()
