import os
import argparse
import functions as func

def main(folder, slice=False, heatmaps=False):
    path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "runs", folder)
    if not os.path.exists(path):
        raise ValueError("{} not found.".format(folder))

    if "_delft3dflow_" in folder:
        extract_inputs = func.extract_data_inputs_delft3dflow
        extract_outputs = func.extract_data_outputs_delft3dflow

    elif "_mitgcm_" in folder:
        extract_inputs = func.extract_data_inputs_mitgcm
        #extract_outputs = func.extract_data_outputs_mitgcm
    else:
        raise ValueError("Unable to recognise model type.")

    input_files = extract_inputs(path, slice)
    func.plot_input_linegraph(input_files)
    if heatmaps:
        print("Saving to file meteorological forcing data as heatmaps")
        func.plot_input_heatmaps(input_files, path)

    output_files = extract_outputs(path)
    func.plot_output_linegraph(output_files)
    if heatmaps:
        print("Saving to file results data as heatmaps")
        func.plot_output_heatmaps(output_files, path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', '-f', help="Model folder in the /run directory", type=str)
    parser.add_argument('--heatmaps', '-m', help="Save input & output heatmaps to files", action='store_true')
    parser.add_argument('--slice', '-s', help='Location for input and output sampling in model units', type=str, default=False)
    args = vars(parser.parse_args())
    main(args["folder"], args["slice"], args["heatmaps"])