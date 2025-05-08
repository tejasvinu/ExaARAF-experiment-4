#!/bin/bash

# --- Job Configuration (Adapt for your HPC Scheduler, e.g., Slurm) ---
#SBATCH --job-name=cpu_monte_carlo_pi
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
PYTHON_EXEC="/home/apps/anaconda3/envs/pytorch-gpu/bin/python"
PIP_EXEC="/home/apps/anaconda3/envs/pytorch-gpu/bin/pip"
CONDA_ENV_NAME="pytorch-gpu"
CONDA_BASE_DIR="/home/apps/anaconda3" # Adjust if your Anaconda/Miniconda base is different

echo "Attempting to activate Conda environment: ${CONDA_ENV_NAME}"
if [ -f "${CONDA_BASE_DIR}/etc/profile.d/conda.sh" ]; then
    # shellcheck disable=SC1091
    source "${CONDA_BASE_DIR}/etc/profile.d/conda.sh"
    conda activate "${CONDA_ENV_NAME}"
    ACTIVATION_STATUS=$?
    if [ $ACTIVATION_STATUS -eq 0 ]; then
        echo "Conda environment '${CONDA_ENV_NAME}' activated successfully."
        # If activation is successful, pip and python should be in PATH
        PYTHON_EXEC="python"
        PIP_EXEC="pip"
    else
        echo "Failed to activate Conda environment '${CONDA_ENV_NAME}' (status: ${ACTIVATION_STATUS}). Falling back to direct paths: ${PYTHON_EXEC}"
    fi
else
    echo "conda.sh not found at ${CONDA_BASE_DIR}/etc/profile.d/conda.sh. Falling back to direct paths: ${PYTHON_EXEC}"
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
OUTPUT_DIR_BASE="cpu_monte_carlo_outputs" # Main directory for all CPU experiments
JOB_OUTPUT_DIR="${OUTPUT_DIR_BASE}/run_${TIMESTAMP}_${SLURM_JOB_ID:-local}"
mkdir -p "${JOB_OUTPUT_DIR}"
echo "Output will be stored in: ${JOB_OUTPUT_DIR}"

# --- System Metrics Logging (Master Node Only) ---
SYSTEM_METRICS_INTERVAL=3 # Log every 3 seconds
LOG_SCRIPT_PATH="./log_system_metrics.py" # Assuming it's in the current directory
LOCAL_LOGGER_PID="" # Initialize PID variable

echo "Starting system metrics logging on the master node only (interval: ${SYSTEM_METRICS_INTERVAL}s)."
LOCAL_NODE_NAME=$(hostname -s) # Get short hostname of the current node

if [ -f "${PYTHON_EXEC}" ] && [ -f "${LOG_SCRIPT_PATH}" ]; then
    "${PYTHON_EXEC}" "${LOG_SCRIPT_PATH}" --output "${JOB_OUTPUT_DIR}/system_metrics_${LOCAL_NODE_NAME}.csv" --interval "${SYSTEM_METRICS_INTERVAL}" &
    LOCAL_LOGGER_PID=$!
    echo "Local system metrics logger started with PID: ${LOCAL_LOGGER_PID} on node ${LOCAL_NODE_NAME}."
    echo "Metrics will be saved to: ${JOB_OUTPUT_DIR}/system_metrics_${LOCAL_NODE_NAME}.csv"
else
    echo "Error: Python executable or log script not found. Cannot start local logger."
    echo "PYTHON_EXEC: ${PYTHON_EXEC}"
    echo "LOG_SCRIPT_PATH: ${LOG_SCRIPT_PATH}"
fi

echo "System metrics logger (local only) launched in the background."

# --- Monte Carlo Task Parameters ---
# Total samples: e.g., 2 nodes * 4 tasks/node * 8 cpus/task * 2,000,000 samples/core = 128,000,000
# Adjust TOTAL_SAMPLES based on the number of cores and desired runtime.
TOTAL_SAMPLES=200000000  # 200 million total samples
MP_BATCH_SIZE=200000      # Batch size for internal multiprocessing within each MPI task

echo "--- Task Configuration ---"
echo "CPU Task Script: cpu_monte_carlo_pi.py"
echo "Total Samples: ${TOTAL_SAMPLES}"
echo "Multiprocessing Batch Size: ${MP_BATCH_SIZE}"
echo "--------------------------"

# --- MPI Execution Command ---
CPU_TASK_SCRIPT_PATH="./cpu_monte_carlo_pi.py" # Assuming it's in the current directory

CMD_TO_RUN="mpirun -np $SLURM_NTASKS ${PYTHON_EXEC} ${CPU_TASK_SCRIPT_PATH} --total-samples ${TOTAL_SAMPLES} --mp-batch-size ${MP_BATCH_SIZE}"

echo "Executing command: ${CMD_TO_RUN}"
echo "--- CPU Task Output START ---"
SECONDS=0 # Bash variable for simple timing
${CMD_TO_RUN}
TASK_EXIT_CODE=$?
DURATION=$SECONDS
echo "--- CPU Task Output END ---"
echo "CPU task finished with exit code: ${TASK_EXIT_CODE}. Duration: ${DURATION} seconds."


# --- Cleanup of Local Background Logger ---
echo "Attempting to stop local system metrics logger (PID: ${LOCAL_LOGGER_PID})..."

if [ -n "$LOCAL_LOGGER_PID" ] && ps -p "$LOCAL_LOGGER_PID" > /dev/null; then
    echo "Sending SIGINT to local logger PID: $LOCAL_LOGGER_PID"
    kill -SIGINT "$LOCAL_LOGGER_PID"
    
    # Wait for up to 5 seconds for clean shutdown
    for i in {1..5}; do
        if ! ps -p "$LOCAL_LOGGER_PID" > /dev/null; then
            echo "Local logger on $(hostname -s) terminated gracefully."
            LOCAL_LOGGER_PID="" # Clear PID
            break
        fi
        sleep 1
    done
    
    # If still running, force kill
    if [ -n "$LOCAL_LOGGER_PID" ] && ps -p "$LOCAL_LOGGER_PID" > /dev/null; then
        echo "Local logger on $(hostname -s) did not terminate gracefully after 5s, sending SIGKILL..."
        kill -SIGKILL "$LOCAL_LOGGER_PID"
    fi
elif [ -n "$LOCAL_LOGGER_PID" ]; then
    echo "Local logger process with PID ${LOCAL_LOGGER_PID} was not found (already terminated or never started properly)."
else
    echo "No local logger PID was set. Nothing to stop."
fi

echo "Local system metrics logging stop sequence completed."


# --- Plot System Metrics (from master node) ---
echo "Plotting system statistics from the master node..."
# The plot script now takes the directory containing all metrics files
PLOT_SCRIPT_PATH="./plot_system_metrics.py"
if [ -d "${JOB_OUTPUT_DIR}" ]; then
    "${PYTHON_EXEC}" "${PLOT_SCRIPT_PATH}" "${JOB_OUTPUT_DIR}"
    PLOT_SYS_EXIT_CODE=$?
    if [ ${PLOT_SYS_EXIT_CODE} -eq 0 ]; then
        echo "System statistics plotted successfully. Plots are in subdirectories within ${JOB_OUTPUT_DIR}."
    else
        echo "Warning: System statistics plotting failed with exit code ${PLOT_SYS_EXIT_CODE}."
    fi
else
    echo "Warning: Job output directory ${JOB_OUTPUT_DIR} not found. Skipping plotting."
fi

# --- Consolidate Run Summary ---
CONSOLIDATED_SUMMARY_FILE="${JOB_OUTPUT_DIR}/consolidated_run_summary_mc_pi.txt"
echo "Creating consolidated run summary: ${CONSOLIDATED_SUMMARY_FILE}"

{
    echo "=== JOB METADATA ==="
    echo "Run Timestamp: ${TIMESTAMP}"
    echo "Job ID (Slurm): ${SLURM_JOB_ID:-N/A}"
    echo "Output Directory: ${JOB_OUTPUT_DIR}"
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
    echo ""

    echo "=== SYSTEM METRICS LOGS (from master node in ${JOB_OUTPUT_DIR}) ==="
    METRICS_FILES_FOUND=0
    for metrics_file in "${JOB_OUTPUT_DIR}"/system_metrics_*.csv; do
        if [ -f "${metrics_file}" ]; then
            echo "--- Log: ${metrics_file} ---"
            cat "${metrics_file}"
            echo -e "\n\n"
            METRICS_FILES_FOUND=$((METRICS_FILES_FOUND + 1))
        fi
    done
    if [ ${METRICS_FILES_FOUND} -eq 0 ]; then
        echo "No system_metrics_*.csv files found in ${JOB_OUTPUT_DIR}."
    fi
    echo -e "\n\n"

    echo "=== CPU TASK SCRIPT (${CPU_TASK_SCRIPT_PATH}) ==="
    if [ -f "${CPU_TASK_SCRIPT_PATH}" ]; then
        cat "${CPU_TASK_SCRIPT_PATH}"
    else
        echo "File not found."
    fi
    echo -e "\n\n"

    echo "=== JOB SCRIPT (this script) ==="
    cat "$0"
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
        echo "=== SLURM JOB STDOUT (head and tail of ${SLURM_OUTPUT_FILE_PATH}) ==="
        echo "--- First 100 lines ---"
        head -n 100 "${SLURM_OUTPUT_FILE_PATH}"
        echo -e "\n--- Last 100 lines ---"
        tail -n 100 "${SLURM_OUTPUT_FILE_PATH}"
    else
        echo "=== SLURM JOB STDOUT ==="
        echo "Slurm job output file not found or path not determined."
    fi

} > "${CONSOLIDATED_SUMMARY_FILE}"

echo "Consolidated summary created: ${CONSOLIDATED_SUMMARY_FILE}"
echo "All outputs are in ${JOB_OUTPUT_DIR}"
echo "Job finished successfully."

exit ${TASK_EXIT_CODE}