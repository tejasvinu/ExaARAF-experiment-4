#!/bin/bash

# --- Job Configuration (Adapt for your HPC Scheduler, e.g., Slurm) ---
#SBATCH --job-name=cpu_monte_carlo_pi_py_log
#SBATCH --nodes=2                # Number of nodes to use
#SBATCH --ntasks-per-node=4      # Number of MPI tasks (processes) per node
#SBATCH --cpus-per-task=8        # Number of CPU cores allocated to each MPI task (for multiprocessing)
#SBATCH --mem-per-cpu=3G         # Memory per allocated CPU core
#SBATCH --time=00:30:00          # Maximum job run time (HH:MM:SS)
#SBATCH --output=cpu_mc_pi_job_%j.out
#SBATCH --error=cpu_mc_pi_job_%j.err
#SBATCH --partition=standard # Specify your HPC's CPU partition

# --- Environment Setup ---
echo "Setting up environment..."
# Example: Load modules for your HPC. These are placeholders.
# module load anaconda3/latest   # Or miniconda
module load openmpi/4.1.1     # Or an MPI implementation compatible with mpi4py
module load anaconda3/anaconda3

# Define Python and Pip executables - default to direct paths
PYTHON_EXEC_FOR_MPI_APP="/home/apps/anaconda3/envs/pytorch-gpu/bin/python" # For the main MPI app
PIP_EXEC="/home/apps/anaconda3/envs/pytorch-gpu/bin/pip"
CONDA_ENV_NAME="pytorch-gpu"
CONDA_BASE_DIR="/home/apps/anaconda3" # Adjust if your Anaconda/Miniconda base is different

# This will be passed to the Python script to use for the logger
# If conda activation works, PYTHON_EXEC_FOR_LOGGER_SCRIPT will be just "python"
# and rely on the PATH being set correctly within the Conda environment.
PYTHON_EXEC_FOR_LOGGER_SCRIPT="/home/apps/anaconda3/envs/pytorch-gpu/bin/python" 

echo "Attempting to activate Conda environment: ${CONDA_ENV_NAME}"
if [ -f "${CONDA_BASE_DIR}/etc/profile.d/conda.sh" ]; then
    # shellcheck disable=SC1091
    source "${CONDA_BASE_DIR}/etc/profile.d/conda.sh"
    conda activate "${CONDA_ENV_NAME}"
    ACTIVATION_STATUS=$?
    if [ $ACTIVATION_STATUS -eq 0 ]; then
        echo "Conda environment '${CONDA_ENV_NAME}' activated successfully."
        PYTHON_EXEC_FOR_MPI_APP="python" # Main app uses python from PATH
        PYTHON_EXEC_FOR_LOGGER_SCRIPT="python" # Logger also uses python from PATH
        PIP_EXEC="pip"
    else
        echo "Failed to activate Conda environment '${CONDA_ENV_NAME}' (status: ${ACTIVATION_STATUS}). Falling back to direct paths: ${PYTHON_EXEC_FOR_MPI_APP}"
    fi
else
    echo "conda.sh not found at ${CONDA_BASE_DIR}/etc/profile.d/conda.sh. Falling back to direct paths: ${PYTHON_EXEC_FOR_MPI_APP}"
fi

# Ensure necessary packages are installed from requirements.txt
echo "Ensuring packages from requirements.txt are installed using ${PIP_EXEC}..."
REQUIREMENTS_FILE="requirements.txt" # Assuming it's in the submission directory
if [ -f "${REQUIREMENTS_FILE}" ]; then
    "${PIP_EXEC}" install --progress-bar off -r "${REQUIREMENTS_FILE}"
    INSTALL_STATUS=$?
    if [ $INSTALL_STATUS -ne 0 ]; then
        echo "WARNING: '${PIP_EXEC} install -r ${REQUIREMENTS_FILE}' failed with status ${INSTALL_STATUS}. Dependency issues may persist."
    else
        echo "Successfully processed ${REQUIREMENTS_FILE}."
    fi
else
    echo "WARNING: ${REQUIREMENTS_FILE} not found. Skipping pip install -r. Ensure dependencies are met in the environment."
fi
echo "Dependency check/installation attempt complete."

echo "--- Slurm Configuration ---"
echo "SLURM_JOB_ID: ${SLURM_JOB_ID:-N/A (not in Slurm job)}"
echo "SLURM_NNODES: ${SLURM_NNODES:-N/A}"
echo "SLURM_NTASKS_PER_NODE: ${SLURM_NTASKS_PER_NODE:-N/A}"
echo "SLURM_CPUS_PER_TASK: ${SLURM_CPUS_PER_TASK:-N/A}"
echo "SLURM_NTASKS (Total MPI tasks): ${SLURM_NTASKS:-N/A}"
echo "--------------------------"

# --- Directory and Experiment Setup ---
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR_BASE="cpu_monte_carlo_outputs_py_log" # Main directory for all CPU experiments
JOB_OUTPUT_DIR_REL="${OUTPUT_DIR_BASE}/run_${TIMESTAMP}_${SLURM_JOB_ID:-local}"
mkdir -p "${JOB_OUTPUT_DIR_REL}"
JOB_OUTPUT_DIR_ABS=$(readlink -f "${JOB_OUTPUT_DIR_REL}")
echo "Output (including logs if enabled) will be stored in: ${JOB_OUTPUT_DIR_ABS}"

# --- System Metrics Logging Parameters for Python Script ---
SYSTEM_METRICS_INTERVAL=3
LOG_SCRIPT_PATH_REL="./log_system_metrics.py" # Relative to SLURM_SUBMIT_DIR

# --- Monte Carlo Task Parameters ---
# Total samples: e.g., 2 nodes * 4 tasks/node * 8 cpus/task * 2,000,000 samples/core = 128,000,000
# Adjust TOTAL_SAMPLES based on the number of cores and desired runtime.
TOTAL_SAMPLES=200000000  # 200 million total samples
MP_BATCH_SIZE=200000      # Batch size for internal multiprocessing within each MPI task

echo "--- Task Configuration ---"
echo "CPU Task Script: cpu_monte_carlo_pi.py"
echo "Total Samples: ${TOTAL_SAMPLES}"
echo "Multiprocessing Batch Size: ${MP_BATCH_SIZE}"
echo "Multi-node logging will be handled by the Python script."
echo "  Logger Script Path (relative to submit dir): ${LOG_SCRIPT_PATH_REL}"
echo "  Python for Logger: ${PYTHON_EXEC_FOR_LOGGER_SCRIPT}"
echo "  Job Output Dir for Logs: ${JOB_OUTPUT_DIR_ABS}"
echo "--------------------------"

# --- MPI Execution Command ---
CPU_TASK_SCRIPT_PATH_REL="./cpu_monte_carlo_pi.py" # Relative to SLURM_SUBMIT_DIR

# Change to SLURM_SUBMIT_DIR so relative paths to scripts are easily resolved by MPI ranks
cd "${SLURM_SUBMIT_DIR}" || { echo "FATAL: Could not cd to SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"; exit 1; }
echo "Changed working directory to: $(pwd)"

CMD_TO_RUN="mpirun -np ${SLURM_NTASKS} \
    ${PYTHON_EXEC_FOR_MPI_APP} ${CPU_TASK_SCRIPT_PATH_REL} \
    --total-samples ${TOTAL_SAMPLES} \
    --mp-batch-size ${MP_BATCH_SIZE} \
    --enable-multi-node-logging \
    --job-output-dir ${JOB_OUTPUT_DIR_ABS} \
    --log-script-path ${LOG_SCRIPT_PATH_REL} \
    --python-exec-for-logger ${PYTHON_EXEC_FOR_LOGGER_SCRIPT} \
    --log-interval ${SYSTEM_METRICS_INTERVAL}"

echo "Executing command: ${CMD_TO_RUN}"
echo "--- CPU Task Output START ---"
SECONDS=0 # Bash variable for simple timing
${CMD_TO_RUN}
TASK_EXIT_CODE=$?
DURATION=$SECONDS
echo "--- CPU Task Output END ---"
echo "CPU task finished with exit code: ${TASK_EXIT_CODE}. Duration: ${DURATION} seconds."

# Logger cleanup is now handled by the Python script's atexit handler.

# --- Plot System Metrics (from master node) ---
echo "Plotting system statistics from the master node..."
# The plot script now takes the directory containing all metrics files
PLOT_SCRIPT_PATH_REL="./plot_system_metrics.py" # Relative to SLURM_SUBMIT_DIR (which is current PWD)

if [ -d "${JOB_OUTPUT_DIR_ABS}" ]; then
    # Ensure PYTHON_EXEC_FOR_MPI_APP is used for plotting as well, assuming it has matplotlib etc.
    "${PYTHON_EXEC_FOR_MPI_APP}" "${PLOT_SCRIPT_PATH_REL}" "${JOB_OUTPUT_DIR_ABS}"
    PLOT_SYS_EXIT_CODE=$?
    if [ ${PLOT_SYS_EXIT_CODE} -eq 0 ]; then
        echo "System statistics plotted successfully. Plots are in subdirectories within ${JOB_OUTPUT_DIR_ABS}."
    else
        echo "Warning: System statistics plotting failed with exit code ${PLOT_SYS_EXIT_CODE}."
    fi
else
    echo "Warning: Job output directory ${JOB_OUTPUT_DIR_ABS} not found. Skipping plotting."
fi

# --- Consolidate Run Summary ---
CONSOLIDATED_SUMMARY_FILE="${JOB_OUTPUT_DIR_ABS}/consolidated_run_summary_mc_pi.txt"
echo "Creating consolidated run summary: ${CONSOLIDATED_SUMMARY_FILE}"

{
    echo "=== JOB METADATA ==="
    echo "Run Timestamp: ${TIMESTAMP}"
    echo "Job ID (Slurm): ${SLURM_JOB_ID:-N/A}"
    echo "Output Directory: ${JOB_OUTPUT_DIR_ABS}"
    echo "Task Duration: ${DURATION} seconds"
    echo "Task Exit Code: ${TASK_EXIT_CODE}"
    echo ""

    echo "=== SLURM CONFIGURATION (from environment) ==="
    echo "SLURM_NNODES: ${SLURM_NNODES:-N/A}"
    echo "SLURM_NTASKS_PER_NODE: ${SLURM_NTASKS_PER_NODE:-N/A}"
    echo "SLURM_CPUS_PER_TASK: ${SLURM_CPUS_PER_TASK:-N/A}"
    echo "SLURM_NTASKS (Total): ${SLURM_NTASKS:-N/A}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR:-N/A}"
    echo ""

    echo "=== TASK PARAMETERS ==="
    echo "Total Samples: ${TOTAL_SAMPLES}"
    echo "Multiprocessing Batch Size: ${MP_BATCH_SIZE}"
    echo "Multi-node Logging Enabled: Yes (via Python script)"
    echo "  Logger Script Path (relative to submit dir): ${LOG_SCRIPT_PATH_REL}"
    echo "  Python for Logger: ${PYTHON_EXEC_FOR_LOGGER_SCRIPT}"
    echo "  Logger Output Dir: ${JOB_OUTPUT_DIR_ABS}"
    echo "  Logger Interval: ${SYSTEM_METRICS_INTERVAL}"
    echo ""

    echo "=== SYSTEM METRICS LOGS (from nodes in ${JOB_OUTPUT_DIR_ABS}) ==="
    METRICS_FILES_FOUND=0
    for metrics_file in "${JOB_OUTPUT_DIR_ABS}"/system_metrics_*.csv; do
        if [ -f "${metrics_file}" ]; then
            echo "--- Log: ${metrics_file} ---"
            head -n 5 "${metrics_file}" # Show only head to keep summary smaller
            echo "..."
            tail -n 5 "${metrics_file}"
            echo -e "\n\n"
            METRICS_FILES_FOUND=$((METRICS_FILES_FOUND + 1))
        fi
    done
    if [ ${METRICS_FILES_FOUND} -eq 0 ]; then
         if [ ! -e "${JOB_OUTPUT_DIR_ABS}/system_metrics_*.csv" ]; then # check if glob pattern itself exists
            echo "No system_metrics_*.csv files found in ${JOB_OUTPUT_DIR_ABS}."
         fi
    fi
    # Also include logger stdout/stderr files
    echo "=== LOGGER STDOUT/STDERR (from nodes in ${JOB_OUTPUT_DIR_ABS}) ==="
    LOGGER_OUT_FILES_FOUND=0
    for logger_out_file in "${JOB_OUTPUT_DIR_ABS}"/logger_out_*.txt; do
        if [ -f "${logger_out_file}" ]; then
            echo "--- Logger Output: ${logger_out_file} ---"
            cat "${logger_out_file}"
            echo -e "\n\n"
            LOGGER_OUT_FILES_FOUND=$((LOGGER_OUT_FILES_FOUND + 1))
        fi
    done
     if [ ${LOGGER_OUT_FILES_FOUND} -eq 0 ]; then
         if [ ! -e "${JOB_OUTPUT_DIR_ABS}/logger_out_*.txt" ]; then
            echo "No logger_out_*.txt files found in ${JOB_OUTPUT_DIR_ABS}."
         fi
    fi
    echo -e "\n\n"

    echo "=== CPU TASK SCRIPT (${CPU_TASK_SCRIPT_PATH_REL}) ==="
    if [ -f "${CPU_TASK_SCRIPT_PATH_REL}" ]; then # Path is now relative to SLURM_SUBMIT_DIR
        cat "${CPU_TASK_SCRIPT_PATH_REL}"
    else
        echo "File not found: ${CPU_TASK_SCRIPT_PATH_REL} (checked from $(pwd))"
    fi
    echo -e "\n\n"

    echo "=== JOB SCRIPT (this script) ==="
    cat "$0" # This will be the original path to the job script
    echo -e "\n\n"
    
    SLURM_OUTPUT_FILE_PATH=""
    if [ -n "$SLURM_JOB_ID" ]; then
        potential_output_file_1="cpu_mc_pi_job_${SLURM_JOB_ID}.out" # Matches #SBATCH --output
        potential_output_file_2="slurm-${SLURM_JOB_ID}.out" 

        if [ -f "$potential_output_file_1" ]; then
            SLURM_OUTPUT_FILE_PATH="$potential_output_file_1"
        elif [ -f "$potential_output_file_2" ]; then
            SLURM_OUTPUT_FILE_PATH="$potential_output_file_2"
        elif [ -f "${SLURM_SUBMIT_DIR}/${potential_output_file_1}" ]; then
             SLURM_OUTPUT_FILE_PATH="${SLURM_SUBMIT_DIR}/${potential_output_file_1}"
        elif [ -f "${SLURM_SUBMIT_DIR}/${potential_output_file_2}" ]; then
             SLURM_OUTPUT_FILE_PATH="${SLURM_SUBMIT_DIR}/${potential_output_file_2}"
        fi
    fi
    
    if [ -n "$SLURM_OUTPUT_FILE_PATH" ] && [ -f "$SLURM_OUTPUT_FILE_PATH" ]; then
        echo "=== SLURM JOB STDOUT (head and tail of $(readlink -f "${SLURM_OUTPUT_FILE_PATH}")) ==="
        echo "--- First 100 lines ---"
        head -n 100 "${SLURM_OUTPUT_FILE_PATH}"
        echo -e "\n--- Last 100 lines ---"
        tail -n 100 "${SLURM_OUTPUT_FILE_PATH}"
    else
        echo "=== SLURM JOB STDOUT ==="
        echo "Slurm job output file not found or path not determined from $(pwd)."
    fi

} > "${CONSOLIDATED_SUMMARY_FILE}"

echo "Consolidated summary created: ${CONSOLIDATED_SUMMARY_FILE}"
echo "All outputs are in ${JOB_OUTPUT_DIR_ABS}"
echo "Job finished successfully."

exit ${TASK_EXIT_CODE}