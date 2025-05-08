# FILE: cpu_monte_carlo_pi.py
import time
import argparse
import random
from multiprocessing import Pool, cpu_count
from mpi4py import MPI
import math
import os
import numpy as np # For efficient random number generation
import subprocess # For launching logger
import signal     # For sending signals to logger
import atexit     # For cleanup

# --- Globals for logger management ---
# Stores Popen object for the logger process if this rank is a node leader
logger_process_object = None
# Stores the output directory, needed by atexit handler
global_job_output_dir = None
# Stores the python executable for the logger, needed by atexit handler
global_python_exec_for_logger = None
# Stores the path to the logger script
global_log_script_path = None
# Stores the interval for logging
global_log_interval = None


def start_node_logger_if_leader(job_output_dir, log_interval, log_script_path, python_exec_for_logger):
    """
    If the current MPI rank is the leader for its node,
    it starts the log_system_metrics.py script.
    """
    global logger_process_object, global_job_output_dir, global_python_exec_for_logger, global_log_script_path, global_log_interval

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    my_hostname = MPI.Get_processor_name()

    # Determine the leader rank for this node (lowest rank on the node)
    all_node_ranks_hostnames = comm.allgather((rank, my_hostname))
    
    min_rank_on_my_node = rank
    for r, h in all_node_ranks_hostnames:
        if h == my_hostname and r < min_rank_on_my_node:
            min_rank_on_my_node = r
    
    is_node_leader = (rank == min_rank_on_my_node)

    if is_node_leader:
        global_job_output_dir = job_output_dir
        global_python_exec_for_logger = python_exec_for_logger
        global_log_script_path = log_script_path
        global_log_interval = log_interval

        log_file = os.path.join(job_output_dir, f"system_metrics_{my_hostname}.csv")
        logger_stdout_file = os.path.join(job_output_dir, f"logger_out_{my_hostname}.txt")
        
        # Ensure the log script path is absolute or resolvable
        # If log_script_path is relative, it's relative to where cpu_monte_carlo_pi.py is run
        abs_log_script_path = os.path.abspath(log_script_path)

        cmd = [
            python_exec_for_logger,
            abs_log_script_path,
            "--output", log_file,
            "--interval", str(log_interval)
        ]
        
        print(f"Rank {rank} (Leader on {my_hostname}): Starting logger. CMD: {' '.join(cmd)}", flush=True)
        print(f"Rank {rank} (Leader on {my_hostname}): Logger stdout/stderr -> {logger_stdout_file}", flush=True)
        
        try:
            with open(logger_stdout_file, 'w') as flog:
                # Detach the logger process so it continues running if the parent (this script)
                # has issues, though we will try to manage it.
                # Using preexec_fn=os.setpgrp to run in its own process group can help
                # if we need to kill the whole group, but for SIGINT to a single PID, it's not strictly needed.
                logger_process_object = subprocess.Popen(cmd, stdout=flog, stderr=subprocess.STDOUT, preexec_fn=os.setpgrp)
            
            print(f"Rank {rank} (Leader on {my_hostname}): Logger started with PID {logger_process_object.pid}", flush=True)
            
            # Register cleanup function to be called at exit for leader ranks
            atexit.register(cleanup_logger_process)
            
        except Exception as e:
            print(f"Rank {rank} (Leader on {my_hostname}): FAILED to start logger: {e}", flush=True)
            logger_process_object = None # Ensure it's None if failed

def cleanup_logger_process():
    """
    Cleans up the logger process. Called by atexit on node leader ranks.
    """
    global logger_process_object
    if logger_process_object and logger_process_object.pid is not None:
        rank = MPI.COMM_WORLD.Get_rank() # Get rank for logging
        my_hostname = MPI.Get_processor_name()
        print(f"Rank {rank} (Leader on {my_hostname}): Cleaning up logger PID {logger_process_object.pid}...", flush=True)
        try:
            # Send SIGINT for graceful shutdown
            os.killpg(os.getpgid(logger_process_object.pid), signal.SIGINT) # Send to process group
            logger_process_object.wait(timeout=10) # Wait up to 10 seconds
            print(f"Rank {rank} (Leader on {my_hostname}): Logger PID {logger_process_object.pid} terminated gracefully after SIGINT.", flush=True)
        except subprocess.TimeoutExpired:
            print(f"Rank {rank} (Leader on {my_hostname}): Logger PID {logger_process_object.pid} did not stop after SIGINT. Sending SIGKILL.", flush=True)
            try:
                os.killpg(os.getpgid(logger_process_object.pid), signal.SIGKILL) # Send to process group
                logger_process_object.wait(timeout=5) # Wait a bit for SIGKILL to take effect
            except Exception as e_kill:
                 print(f"Rank {rank} (Leader on {my_hostname}): Error sending SIGKILL to logger PID {logger_process_object.pid}: {e_kill}", flush=True)
        except Exception as e:
            print(f"Rank {rank} (Leader on {my_hostname}): Error during logger cleanup for PID {logger_process_object.pid}: {e}", flush=True)
        finally:
            logger_process_object = None


def monte_carlo_pi_batch(num_samples_in_batch):
    """
    Performs a batch of Monte Carlo samples to estimate Pi.
    Generates random points in a 1x1 square and counts how many fall
    within a quarter circle of radius 1.
    Pi is then estimated as 4 * (points_in_circle / total_points).
    """
    points_in_circle = 0
    # Using numpy for potentially faster random number generation in a batch
    # For very large num_samples_in_batch, this can be more efficient than a loop of random.random()
    # However, for many small calls, the overhead might not be worth it.
    # We'll stick to a loop for simplicity and to ensure CPU work per sample.
    for _ in range(num_samples_in_batch):
        x = random.uniform(0, 1)
        y = random.uniform(0, 1)
        if x*x + y*y <= 1.0:
            points_in_circle += 1
    return points_in_circle

def main():
    parser = argparse.ArgumentParser(description="CPU-intensive Monte Carlo Pi estimation using MPI and multiprocessing.")
    parser.add_argument('--total-samples', type=int, default=10**8, help='Total number of samples to generate across all processes.')
    parser.add_argument('--mp-batch-size', type=int, default=10**5, help='Number of samples processed by each multiprocessing worker in a single call.')
    
    # Arguments for logger management
    parser.add_argument('--enable-multi-node-logging', action='store_true', help='Enable system metrics logging on each node by MPI leaders.')
    parser.add_argument('--job-output-dir', type=str, default=".", help='Directory to store outputs, including logger CSVs.')
    parser.add_argument('--log-script-path', type=str, default="./log_system_metrics.py", help='Path to the log_system_metrics.py script.')
    parser.add_argument('--python-exec-for-logger', type=str, default="python", help='Python executable to use for launching the logger script (e.g., /path/to/conda/env/bin/python).')
    parser.add_argument('--log-interval', type=int, default=3, help='Logging interval in seconds for system metrics.')
    
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size() # Total number of MPI processes

    # Create the output directory if it doesn't exist (all ranks attempt, but it's fine)
    # This should ideally be an absolute path passed from the job script.
    if rank == 0: # Let rank 0 create it to avoid race conditions, though makedirs is often safe.
        if args.enable_multi_node_logging and args.job_output_dir:
            os.makedirs(args.job_output_dir, exist_ok=True)
            print(f"Rank 0: Output directory for logging: {os.path.abspath(args.job_output_dir)}", flush=True)
    comm.Barrier() # Ensure directory is created before leaders try to write log_out files.

    # Start logger if enabled and this rank is a node leader
    if args.enable_multi_node_logging:
        start_node_logger_if_leader(
            job_output_dir=os.path.abspath(args.job_output_dir), # Use absolute path
            log_interval=args.log_interval,
            log_script_path=args.log_script_path, # This will be made absolute in the function
            python_exec_for_logger=args.python_exec_for_logger
        )
    
    comm.Barrier() # Ensure all loggers attempt to start before proceeding

    num_local_workers = int(os.getenv('SLURM_CPUS_PER_TASK', cpu_count()))
    if num_local_workers <= 0:
        num_local_workers = 1

    if rank == 0:
        print(f"--- Monte Carlo Pi Estimation ---", flush=True)
        print(f"MPI World Size (Total MPI Ranks): {size}", flush=True)
        print(f"Total samples to generate: {args.total_samples}", flush=True)
        print(f"Multiprocessing batch size per worker: {args.mp_batch_size}", flush=True)
        print(f"Multiprocessing workers per MPI rank: ~{num_local_workers}", flush=True)
        if args.enable_multi_node_logging:
            print(f"Multi-node system logging is ENABLED.", flush=True)
            print(f"  Logger script: {args.log_script_path}", flush=True)
            print(f"  Logger Python: {args.python_exec_for_logger}", flush=True)
            print(f"  Logger output dir: {os.path.abspath(args.job_output_dir)}", flush=True)
        else:
            print(f"Multi-node system logging is DISABLED.", flush=True)
        print(f"--- Starting Simulation ---", flush=True)

    # Distribute the total samples among MPI processes
    samples_per_rank = args.total_samples // size
    remainder_samples = args.total_samples % size

    # Assign remaining samples to the first few ranks
    if rank < remainder_samples:
        my_samples = samples_per_rank + 1
    else:
        my_samples = samples_per_rank

    if my_samples == 0:
        print(f"Rank {rank}: No samples to process.", flush=True)
        comm.Barrier()
        # Gather 0 points in circle and 0 total samples for this rank
        rank_results = (0, 0)
        all_rank_results = comm.gather(rank_results, root=0)
        if rank == 0:
             print("\n--- Results ---", flush=True)
             print("No samples processed by any rank.", flush=True)
        return # Important to return so atexit cleanup runs for leaders if they started loggers

    rank_local_points_in_circle = 0
    start_time_rank_work = time.time()

    if num_local_workers > 1 and my_samples > args.mp_batch_size : # Use multiprocessing if beneficial
        try:
            pool = Pool(processes=num_local_workers)
            
            # Create a list of batch sizes for the workers
            # Each element in batches_for_pool is the number of samples for one call to monte_carlo_pi_batch
            num_full_batches = my_samples // args.mp_batch_size
            remaining_for_last_batch = my_samples % args.mp_batch_size
            
            batches_for_pool = [args.mp_batch_size] * num_full_batches
            if remaining_for_last_batch > 0:
                batches_for_pool.append(remaining_for_last_batch)

            if batches_for_pool:
                # Each call to monte_carlo_pi_batch returns the count of points in circle for that batch
                results_from_pool = pool.map(monte_carlo_pi_batch, batches_for_pool)
                rank_local_points_in_circle = sum(results_from_pool)
            
            pool.close()
            pool.join()
        except Exception as e:
            print(f"Rank {rank}: Error during multiprocessing: {e}. Falling back to serial.", flush=True)
            # Fallback to serial execution for this rank's samples
            rank_local_points_in_circle = monte_carlo_pi_batch(my_samples)
    else:
        # Single process (no multiprocessing pool) or too few samples for effective batching
        rank_local_points_in_circle = monte_carlo_pi_batch(my_samples)
    
    end_time_rank_work = time.time()
    
    print(f"Rank {rank}: Processed {my_samples} samples. Found {rank_local_points_in_circle} points in circle. Time: {end_time_rank_work - start_time_rank_work:.3f}s", flush=True)

    # Synchronize before gathering results
    comm.Barrier()

    # Each rank sends a tuple: (points_in_circle_for_this_rank, samples_processed_by_this_rank)
    rank_results = (rank_local_points_in_circle, my_samples)
    all_rank_results = comm.gather(rank_results, root=0)

    if rank == 0:
        total_points_in_circle_overall = 0
        total_samples_overall = 0
        for r_points, r_samples in all_rank_results:
            total_points_in_circle_overall += r_points
            total_samples_overall += r_samples
        
        estimated_pi = 0
        if total_samples_overall > 0:
            estimated_pi = 4.0 * total_points_in_circle_overall / total_samples_overall
        
        print(f"\n--- Results ---", flush=True)
        print(f"Total samples processed across all ranks: {total_samples_overall}", flush=True)
        print(f"Total points found in circle across all ranks: {total_points_in_circle_overall}", flush=True)
        print(f"Estimated value of Pi: {estimated_pi:.8f}", flush=True)
        print(f"Error from math.pi: {abs(estimated_pi - math.pi):.8f}", flush=True)
        print(f"Rank 0: Aggregation complete. Main MPI application will now exit.", flush=True)
        # atexit handlers on leader nodes should now trigger to clean up loggers.

if __name__ == "__main__":
    main()
    # MPI_Finalize() is typically called automatically by mpi4py when the script ends.
    # If explicit MPI_Finalize is needed, ensure atexit cleanup happens before or is robust.