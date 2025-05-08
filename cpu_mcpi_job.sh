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

# --- System Metrics Logging (All Nodes) ---
SYSTEM_METRICS_INTERVAL=3 # Log every 3 seconds
LOG_SCRIPT_PATH="./log_system_metrics.py" # Assuming it's in the current directory

echo "Starting system metrics logging on all allocated nodes (interval: ${SYSTEM_METRICS_INTERVAL}s)."

# Create a small wrapper script that each node will execute to capture its own hostname
cat > "${JOB_OUTPUT_DIR}/run_logger.sh" << 'EOF'
#!/bin/bash
NODE_NAME=$(hostname)
OUTPUT_DIR=$1
INTERVAL=$2
LOG_SCRIPT=$3
PYTHON_PATH=$4

echo "Starting logger on node: ${NODE_NAME}"
"${PYTHON_PATH}" "${LOG_SCRIPT}" --output "${OUTPUT_DIR}/system_metrics_${NODE_NAME}.csv" --interval "${INTERVAL}"
EOF

chmod +x "${JOB_OUTPUT_DIR}/run_logger.sh"

# Launch one logger per node using the wrapper script
srun --ntasks=${SLURM_NNODES} --ntasks-per-node=1 --overlap \
    "${JOB_OUTPUT_DIR}/run_logger.sh" "${JOB_OUTPUT_DIR}" "${SYSTEM_METRICS_INTERVAL}" "${LOG_SCRIPT_PATH}" "/home/apps/anaconda3/envs/pytorch-gpu/bin/python" &

echo "System metrics loggers launched via srun in the background."

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

CMD_TO_RUN="mpirun -np $SLURM_NTASKS /home/apps/anaconda3/envs/pytorch-gpu/bin/python ${CPU_TASK_SCRIPT_PATH} --total-samples ${TOTAL_SAMPLES} --mp-batch-size ${MP_BATCH_SIZE}"

echo "Executing command: ${CMD_TO_RUN}"
echo "--- CPU Task Output START ---"
SECONDS=0 # Bash variable for simple timing
${CMD_TO_RUN}
TASK_EXIT_CODE=$?
DURATION=$SECONDS
echo "--- CPU Task Output END ---"
echo "CPU task finished with exit code: ${TASK_EXIT_CODE}. Duration: ${DURATION} seconds."


# --- Cleanup of Background Loggers (All Nodes) ---
echo "Attempting to stop system metrics loggers on all nodes..."

# Create a cleanup script that each node will run to find and stop its logger
cat > "${JOB_OUTPUT_DIR}/stop_logger.sh" << 'EOF'
#!/bin/bash
NODE_NAME=$(hostname)
PYTHON_PATH=$1
echo "Stopping logger on ${NODE_NAME}..."

# Find the Python process running the log_system_metrics.py script
# Using the specific Python path when searching
LOGGER_PID=$(ps -ef | grep "${PYTHON_PATH}.*log_system_metrics.py" | grep -v grep | awk '{print $2}')

if [ -n "$LOGGER_PID" ]; then
    echo "Found logger process on ${NODE_NAME} with PID: ${LOGGER_PID}, sending SIGINT..."
    kill -SIGINT $LOGGER_PID
    
    # Wait for up to 5 seconds for clean shutdown
    for i in {1..5}; do
        if ! ps -p $LOGGER_PID > /dev/null; then
            echo "Logger on ${NODE_NAME} terminated gracefully."
            exit 0
        fi
        sleep 1
    done
    
    # If still running, force kill
    if ps -p $LOGGER_PID > /dev/null; then
        echo "Logger on ${NODE_NAME} did not terminate gracefully after 5s, sending SIGKILL..."
        kill -SIGKILL $LOGGER_PID
    fi
else
    echo "No logger process found on ${NODE_NAME}."
fi
EOF

chmod +x "${JOB_OUTPUT_DIR}/stop_logger.sh"

# Run the cleanup script on all nodes
srun --ntasks=${SLURM_NNODES} --ntasks-per-node=1 "${JOB_OUTPUT_DIR}/stop_logger.sh" "/home/apps/anaconda3/envs/pytorch-gpu/bin/python"

echo "System metrics logging stop sequence completed."


# --- Plot System Metrics (from all nodes) ---
echo "Plotting system statistics from all nodes..."
# The plot script now takes the directory containing all metrics files
PLOT_SCRIPT_PATH="./plot_system_metrics.py"
if [ -d "${JOB_OUTPUT_DIR}" ]; then
    /home/apps/anaconda3/envs/pytorch-gpu/bin/python "${PLOT_SCRIPT_PATH}" "${JOB_OUTPUT_DIR}"
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

    echo "=== SYSTEM METRICS LOGS (from all nodes in ${JOB_OUTPUT_DIR}) ==="
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