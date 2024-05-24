from datasets import load_dataset

ds = load_dataset("togethercomputer/RedPajama-Data-V2",
                  name="default",
                  partition="head_middle",
                  snapshots=["2023-06", "2022-49"],
                  languages=["en"]) 