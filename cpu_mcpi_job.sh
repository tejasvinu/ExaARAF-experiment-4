#!/bin/bash

# --- Job Configuration (Adapt for your HPC Scheduler, e.g., Slurm) ---
# #SBATCH --job-name=cpu_monte_carlo_pi
# #SBATCH --nodes=2                # Number of nodes to use
# #SBATCH --ntasks-per-node=4      # Number of MPI tasks (processes) per node
# #SBATCH --cpus-per-task=8        # Number of CPU cores allocated to each MPI task (for multiprocessing)
# #SBATCH --mem-per-cpu=2G         # Memory per allocated CPU core
# #SBATCH --time=00:30:00          # Maximum job run time (HH:MM:SS)
# #SBATCH --output=cpu_mc_pi_job_%j.out
# #SBATCH --error=cpu_mc_pi_job_%j.err
# #SBATCH --partition=cpu_partition # Specify your HPC's CPU partition

# --- Environment Setup ---
echo "Setting up environment..."
# Example: Load modules for your HPC. These are placeholders.
# module load anaconda3/latest   # Or miniconda
module load openmpi/4.1.1     # Or an MPI implementation compatible with mpi4py
module load anaconda3/anaconda
# Activate your Conda environment (adjust path and name)
# source /path/to/your/conda/etc/profile.d/conda.sh
# conda activate your_python_mpi_env

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

# --- System Metrics Logging ---
SYSTEM_METRICS_LOG_FILE="${JOB_OUTPUT_DIR}/system_metrics_primary_node.csv"
SYSTEM_METRICS_INTERVAL=3 # Log every 3 seconds
echo "Starting system metrics logging to ${SYSTEM_METRICS_LOG_FILE} (interval: ${SYSTEM_METRICS_INTERVAL}s)."
python log_system_metrics.py --output "${SYSTEM_METRICS_LOG_FILE}" --interval "${SYSTEM_METRICS_INTERVAL}" &
SYSTEM_METRICS_PID=$!
echo "System metrics logger started with PID: ${SYSTEM_METRICS_PID}"

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

CMD_TO_RUN="srun python ${CPU_TASK_SCRIPT_PATH} --total-samples ${TOTAL_SAMPLES} --mp-batch-size ${MP_BATCH_SIZE}"

echo "Executing command: ${CMD_TO_RUN}"
echo "--- CPU Task Output START ---"
SECONDS=0 # Bash variable for simple timing
${CMD_TO_RUN}
TASK_EXIT_CODE=$?
DURATION=$SECONDS
echo "--- CPU Task Output END ---"
echo "CPU task finished with exit code: ${TASK_EXIT_CODE}. Duration: ${DURATION} seconds."


# --- Cleanup of Background Loggers ---
if [ -n "$SYSTEM_METRICS_PID" ]; then
    echo "Stopping system metrics logging (PID: ${SYSTEM_METRICS_PID})..."
    kill -SIGINT ${SYSTEM_METRICS_PID}
    
    WAIT_TIMEOUT_SECONDS=10
    elapsed=0
    while ps -p ${SYSTEM_METRICS_PID} > /dev/null && [ $elapsed -lt $WAIT_TIMEOUT_SECONDS ]; do
        sleep 1
        elapsed=$((elapsed + 1))
    done

    if ps -p ${SYSTEM_METRICS_PID} > /dev/null; then
        echo "System metrics logger (PID: ${SYSTEM_METRICS_PID}) did not stop gracefully after ${WAIT_TIMEOUT_SECONDS}s. Sending SIGKILL."
        kill -SIGKILL ${SYSTEM_METRICS_PID}
    fi
    wait ${SYSTEM_METRICS_PID} 2>/dev/null
    echo "System metrics logging stopped."
fi

# --- Plot System Metrics ---
echo "Plotting system statistics..."
if [ -f "${SYSTEM_METRICS_LOG_FILE}" ]; then
    python plot_system_metrics.py "${SYSTEM_METRICS_LOG_FILE}"
    PLOT_SYS_EXIT_CODE=$?
    if [ ${PLOT_SYS_EXIT_CODE} -eq 0 ]; then
        echo "System statistics plotted successfully to ${JOB_OUTPUT_DIR}."
    else
        echo "Warning: System statistics plotting failed with exit code ${PLOT_SYS_EXIT_CODE}."
    fi
else
    echo "Warning: System metrics log file ${SYSTEM_METRICS_LOG_FILE} not found. Skipping plotting."
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

    echo "=== SYSTEM METRICS LOG (${SYSTEM_METRICS_LOG_FILE}) ==="
    if [ -f "${SYSTEM_METRICS_LOG_FILE}" ]; then
        cat "${SYSTEM_METRICS_LOG_FILE}"
    else
        echo "File not found."
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