import json
import os
N_FILES_MAX_PER_SAMPLE = 1
# Function to extract samples from JSON and generate the necessary .txt files
def extract_samples_from_json(json_file):
    output_files = []
    
    with open(json_file, "r") as fd:
        data = json.load(fd)

        for sample, conditions in data.items():
            for condition, details in conditions.items():
                # Creating a filename for the sample and condition
                sample_name = f"{sample}__{condition}"
                output_files.append((sample, condition))
                
                # Write paths to a .txt file with the correct path replacement
                with open(f"sample_{sample_name}_paths.txt", "w") as path_file:
                    paths = [file_info["path"] for file_info in details["files"]]
                    path_file.write("\n".join(paths))
    return output_files

# Function to get file paths based on the index
def get_file_paths(wildcards, max=N_FILES_MAX_PER_SAMPLE):
    "Return list of at most MAX file paths for the given SAMPLE and CONDITION."
    filepaths = []
    with open(f"sample_{wildcards.sample}__{wildcards.condition}_paths.txt", "r") as fd:
        filepaths = fd.read().splitlines()
    
    # Use the index as the wildcard, creating a path for each file based on its index
    return [f"histograms/histograms_{wildcards.sample}__{wildcards.condition}__{index}.root" for index in range(len(filepaths))][:max]

sample_conditions = extract_samples_from_json("file_inputs_servicex.json")

rule all:
    input:
        "histograms_merged.root"

rule process_sample_one_file_in_sample:
    container:
        "povstenandrii/ttbarkerberos:20240311"
    resources:
        kubernetes_memory_limit="8000Mi"
    input:
        "ttbar_analysis_reana.ipynb"
    output:
        "histograms/histograms_{sample}__{condition}__{index}.root"
    params:
        sample_name = '{sample}__{condition}'
    shell:
        "/bin/bash -l && source fix-env.sh && python prepare_workspace.py sample_{params.sample_name}_{wildcards.index} && papermill ttbar_analysis_reana.ipynb sample_{params.sample_name}_{wildcards.index}_out.ipynb -p sample_name {params.sample_name} -p index {wildcards.index} -k python3"

rule process_sample:
    container:
        "povstenandrii/merged_povsten:20240215"
    resources:
        kubernetes_memory_limit="1850Mi"
    input:
        "file_merging.ipynb",
        get_file_paths
    output:
        "everything_merged_{sample}__{condition}.root"
    params:
        sample_name = '{sample}__{condition}'
    shell:
        "papermill file_merging.ipynb merged_{params.sample_name}.ipynb -p sample_name {params.sample_name} -k python3"

rule merging_histograms:
    container:
        "povstenandrii/ttbarkerberos:20240311"
    resources:
        kubernetes_memory_limit="1850Mi"
    input:
        "everything_merged_ttbar__nominal.root",
        "everything_merged_ttbar__ME_var.root",
        "everything_merged_ttbar__PS_var.root",
        "everything_merged_ttbar__scaleup.root",
        "everything_merged_ttbar__scaledown.root",
        "everything_merged_single_top_s_chan__nominal.root",
        "everything_merged_single_top_t_chan__nominal.root",
        "everything_merged_single_top_tW__nominal.root",
        "everything_merged_wjets__nominal.root",
        "final_merging.ipynb"
    output:
        "histograms_merged.root"
    shell:
        "/bin/bash -l && source fix-env.sh && papermill final_merging.ipynb result_notebook.ipynb -k python3"
