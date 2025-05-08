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
module load openmpi/4.1.1
module load anaconda3/anaconda3

PYTHON_EXEC="/home/apps/anaconda3/envs/pytorch-gpu/bin/python"
PIP_EXEC="/home/apps/anaconda3/envs/pytorch-gpu/bin/pip"
CONDA_ENV_NAME="pytorch-gpu"
CONDA_BASE_DIR="/home/apps/anaconda3"
# This specific Python interpreter will be used for remote SSH commands
REMOTE_PYTHON_INTERPRETER="/home/apps/anaconda3/envs/pytorch-gpu/bin/python"


echo "Attempting to activate Conda environment: ${CONDA_ENV_NAME}"
if [ -f "${CONDA_BASE_DIR}/etc/profile.d/conda.sh" ]; then
    # shellcheck disable=SC1091
    source "${CONDA_BASE_DIR}/etc/profile.d/conda.sh"
    conda activate "${CONDA_ENV_NAME}"
    ACTIVATION_STATUS=$?
    if [ $ACTIVATION_STATUS -eq 0 ]; then
        echo "Conda environment '${CONDA_ENV_NAME}' activated successfully."
        PYTHON_EXEC="python" # Use 'python' from PATH if env activated
        PIP_EXEC="pip"       # Use 'pip' from PATH if env activated
    else
        echo "Failed to activate Conda environment '${CONDA_ENV_NAME}' (status: ${ACTIVATION_STATUS}). Falling back to direct paths: ${PYTHON_EXEC}"
    fi
else
    echo "conda.sh not found at ${CONDA_BASE_DIR}/etc/profile.d/conda.sh. Falling back to direct paths: ${PYTHON_EXEC}"
fi

echo "Ensuring packages from requirements.txt are installed using ${PIP_EXEC}..."
REQUIREMENTS_FILE="requirements.txt"
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
# Ensure OUTPUT_DIR_BASE is an absolute path using SLURM_SUBMIT_DIR
if [ -n "${SLURM_SUBMIT_DIR}" ]; then
    OUTPUT_DIR_BASE="${SLURM_SUBMIT_DIR}/cpu_monte_carlo_outputs"
else
    # Fallback if SLURM_SUBMIT_DIR is somehow not set, though it should be in a Slurm job
    echo "Warning: SLURM_SUBMIT_DIR is not set. Using relative path for OUTPUT_DIR_BASE."
    OUTPUT_DIR_BASE="cpu_monte_carlo_outputs"
fi
JOB_OUTPUT_DIR="${OUTPUT_DIR_BASE}/run_${TIMESTAMP}_${SLURM_JOB_ID:-local}"
mkdir -p "${JOB_OUTPUT_DIR}"
echo "Output will be stored in: ${JOB_OUTPUT_DIR}" # This will now be an absolute path

# --- System Metrics Logging (All Nodes via SSH) ---
SYSTEM_METRICS_INTERVAL=3
LOG_SCRIPT_PATH="./log_system_metrics.py" # Assuming it's in the current directory, accessible via shared FS

echo "Attempting to start system metrics logging on all allocated nodes via SSH (interval: ${SYSTEM_METRICS_INTERVAL}s)."

if [ -n "$SLURM_JOB_NODELIST" ]; then
    NODE_LIST=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
    echo "Target nodes for logging: ${NODE_LIST}"
    for node in ${NODE_LIST}; do
        echo "Attempting to start logger on node: ${node}"
        REMOTE_METRICS_FILE="${JOB_OUTPUT_DIR}/system_metrics_${node}.csv"
        REMOTE_LOGGER_STDOUT="${JOB_OUTPUT_DIR}/logger_stdout_${node}.txt"
        REMOTE_LOGGER_STDERR="${JOB_OUTPUT_DIR}/logger_stderr_${node}.txt"

        # Ensure the log script path is absolute if it's relative to SLURM_SUBMIT_DIR
        ABS_LOG_SCRIPT_PATH="${SLURM_SUBMIT_DIR}/${LOG_SCRIPT_PATH}"
        if [[ "$LOG_SCRIPT_PATH" == /* ]]; then # if already absolute
            ABS_LOG_SCRIPT_PATH="$LOG_SCRIPT_PATH"
        fi

        # User's local site-packages, determined from previous output
        USER_LOCAL_SITE_PACKAGES="/home/poweropt1/.local/lib/python3.10/site-packages"

        REMOTE_COMMAND="export PYTHONPATH=${USER_LOCAL_SITE_PACKAGES}${PYTHONPATH:+:$PYTHONPATH}; nohup ${REMOTE_PYTHON_INTERPRETER} ${ABS_LOG_SCRIPT_PATH} --output ${REMOTE_METRICS_FILE} --interval ${SYSTEM_METRICS_INTERVAL} > ${REMOTE_LOGGER_STDOUT} 2> ${REMOTE_LOGGER_STDERR} &"
        
        echo "Executing on ${node}: ${REMOTE_COMMAND}" # Added for better debugging
        ssh -n -f "${node}" "${REMOTE_COMMAND}"
        SSH_EXIT_CODE=$?
        if [ ${SSH_EXIT_CODE} -eq 0 ]; then
            echo "Logger launch command sent to ${node} successfully."
        else
            echo "Error: Failed to send logger launch command to ${node} via SSH (exit code: ${SSH_EXIT_CODE}). Check SSH connectivity and permissions."
        fi
    done
else
    echo "Warning: SLURM_JOB_NODELIST is not set. Cannot determine target nodes for SSH logging."
fi
echo "System metrics logger launch sequence (via SSH) initiated."