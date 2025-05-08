import time
import argparse
import random
from multiprocessing import Pool, cpu_count
from mpi4py import MPI
import math
import os
import numpy as np # For efficient random number generation

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
        if math.sqrt(x*x + y*y) <= 1.0:
            points_in_circle += 1
    return points_in_circle

def main():
    parser = argparse.ArgumentParser(description="CPU-intensive Monte Carlo Pi estimation using MPI and multiprocessing.")
    parser.add_argument('--total-samples', type=int, default=int(4e9), help='Total number of samples to generate across all processes.')
    parser.add_argument('--mp-batch-size', type=int, default=10**5, help='Number of samples processed by each multiprocessing worker in a single call.')
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size() # Total number of MPI processes

    num_local_workers = int(os.getenv('SLURM_CPUS_PER_TASK', cpu_count()))
    if num_local_workers <= 0:
        num_local_workers = 1

    if rank == 0:
        print(f"--- Monte Carlo Pi Estimation ---", flush=True)
        print(f"MPI World Size (Total MPI Ranks): {size}", flush=True)
        print(f"Total samples to generate: {args.total_samples}", flush=True)
        print(f"Multiprocessing batch size per worker: {args.mp_batch_size}", flush=True)
        print(f"Multiprocessing workers per MPI rank: ~{num_local_workers}", flush=True)
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
        return


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
        print(f"Estimated value of Pi: {estimated_pi:.12f}", flush=True)
        print(f"Error from math.pi: {abs(estimated_pi - math.pi):.12f}", flush=True)
        print(f"Rank 0: Aggregation complete.", flush=True)

if __name__ == "__main__":
    main()
