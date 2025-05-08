import matplotlib
matplotlib.use('Agg') # Use non-interactive backend
import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os
import glob # Added for finding files

def plot_single_node_metrics(csv_filepath, node_identifier, base_output_dir):
    """
    Reads system metrics from a single CSV file and generates plots for that node.
    Saves plots in a subdirectory named after the node_identifier within base_output_dir.
    """
    try:
        df = pd.read_csv(csv_filepath)
    except FileNotFoundError:
        print(f"Error: System metrics file not found at {csv_filepath}")
        return 0 # Return count of plots generated
    except pd.errors.EmptyDataError:
        print(f"Error: No data in system metrics file {csv_filepath}")
        return 0
    except Exception as e:
        print(f"Error reading CSV file {csv_filepath}: {e}")
        return 0

    if df.empty:
        print(f"Warning: System metrics file {csv_filepath} is empty. No plots will be generated.")
        return 0

    if 'timestamp' not in df.columns:
        print(f"Error: 'timestamp' column not found in {csv_filepath}. Available columns: {df.columns.tolist()}")
        return 0

    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['elapsed_time_s'] = (df['timestamp'] - df['timestamp'].iloc[0]).dt.total_seconds()
    except Exception as e:
        print(f"Error processing timestamp column in {csv_filepath}: {e}. Skipping plots that require time.")
        return 0

    # Create a specific output directory for this node's plots
    node_plot_dir = os.path.join(base_output_dir, f"plots_{node_identifier}")
    os.makedirs(node_plot_dir, exist_ok=True)

    plots_generated_count = 0

    # CPU Utilization
    if 'cpu_percent' in df.columns:
        try:
            plt.figure(figsize=(12, 6))
            plt.plot(df['elapsed_time_s'], df['cpu_percent'], label=f'CPU Utilization ({node_identifier})')
            plt.xlabel('Time (seconds)')
            plt.ylabel('CPU Utilization (%)')
            plt.title(f'CPU Utilization Over Time ({node_identifier})')
            plt.legend()
            plt.grid(True)
            plot_path = os.path.join(node_plot_dir, f'system_cpu_utilization_{node_identifier}.png')
            plt.savefig(plot_path)
            plt.close()
            print(f"Saved plot: {plot_path}")
            plots_generated_count += 1
        except Exception as e:
            print(f"Could not generate plot for cpu_percent on {node_identifier}: {e}")
    else:
        print(f"Column 'cpu_percent' not found in {csv_filepath}, skipping CPU plot for {node_identifier}.")

    # Memory Usage
    if 'memory_percent' in df.columns and 'memory_available_gb' in df.columns and 'memory_total_gb' in df.columns:
        try:
            fig, ax1 = plt.subplots(figsize=(12, 6))
            color = 'tab:red'
            ax1.set_xlabel('Time (seconds)')
            ax1.set_ylabel(f'Memory Utilization (%) - {node_identifier}', color=color)
            ax1.plot(df['elapsed_time_s'], df['memory_percent'], color=color, label=f'Memory Utilization (%) ({node_identifier})')
            ax1.tick_params(axis='y', labelcolor=color)
            ax1.legend(loc='upper left')

            ax2 = ax1.twinx()
            color = 'tab:blue'
            ax2.set_ylabel(f'Memory Available (GB) - {node_identifier}', color=color)
            ax2.plot(df['elapsed_time_s'], df['memory_available_gb'], color=color, linestyle='--', label=f'Memory Available (GB) ({node_identifier})')
            if df['memory_total_gb'].nunique() == 1:
                 total_mem_gb = df['memory_total_gb'].iloc[0]
                 ax2.axhline(y=total_mem_gb, color='tab:green', linestyle=':', label=f'Total Memory ({total_mem_gb:.2f} GB) ({node_identifier})')
            ax2.tick_params(axis='y', labelcolor=color)
            ax2.legend(loc='upper right')

            fig.tight_layout()
            plt.title(f'System Memory Usage Over Time ({node_identifier})')
            plt.grid(True)
            plot_path = os.path.join(node_plot_dir, f'system_memory_usage_{node_identifier}.png')
            plt.savefig(plot_path)
            plt.close()
            print(f"Saved plot: {plot_path}")
            plots_generated_count += 1
        except Exception as e:
            print(f"Could not generate plot for memory usage on {node_identifier}: {e}")
    else:
        print(f"Memory columns not found in {csv_filepath}, skipping memory plot for {node_identifier}.")

    # Disk I/O Throughput
    if 'disk_read_mb_interval' in df.columns and 'disk_write_mb_interval' in df.columns:
        try:
            plt.figure(figsize=(12, 6))
            plt.plot(df['elapsed_time_s'], df['disk_read_mb_interval'], label=f'Disk Read (MB/interval) ({node_identifier})')
            plt.plot(df['elapsed_time_s'], df['disk_write_mb_interval'], label=f'Disk Write (MB/interval) ({node_identifier})')
            plt.xlabel('Time (seconds)')
            plt.ylabel('Data Transferred (MB per interval)')
            plt.title(f'Disk I/O Throughput Over Time ({node_identifier})')
            plt.legend()
            plt.grid(True)
            plot_path = os.path.join(node_plot_dir, f'system_disk_io_throughput_{node_identifier}.png')
            plt.savefig(plot_path)
            plt.close()
            print(f"Saved plot: {plot_path}")
            plots_generated_count += 1
        except Exception as e:
            print(f"Could not generate plot for disk I/O throughput on {node_identifier}: {e}")
    else:
        print(f"Disk I/O columns not found in {csv_filepath}, skipping disk I/O throughput plot for {node_identifier}.")

    # Network I/O Throughput
    if 'net_sent_mb_interval' in df.columns and 'net_recv_mb_interval' in df.columns:
        try:
            plt.figure(figsize=(12, 6))
            plt.plot(df['elapsed_time_s'], df['net_sent_mb_interval'], label=f'Network Sent (MB/interval) ({node_identifier})')
            plt.plot(df['elapsed_time_s'], df['net_recv_mb_interval'], label=f'Network Received (MB/interval) ({node_identifier})')
            plt.xlabel('Time (seconds)')
            plt.ylabel('Data Transferred (MB per interval)')
            plt.title(f'Network I/O Throughput Over Time ({node_identifier})')
            plt.legend()
            plt.grid(True)
            plot_path = os.path.join(node_plot_dir, f'system_network_io_throughput_{node_identifier}.png')
            plt.savefig(plot_path)
            plt.close()
            print(f"Saved plot: {plot_path}")
            plots_generated_count += 1
        except Exception as e:
            print(f"Could not generate plot for network I/O throughput on {node_identifier}: {e}")
    else:
        print(f"Network I/O columns not found in {csv_filepath}, skipping network I/O throughput plot for {node_identifier}.")
    
    return plots_generated_count

def main():
    parser = argparse.ArgumentParser(description='Plot system metrics from CSV log files in a directory.')
    parser.add_argument('metrics_directory', type=str, help='Path to the directory containing system metrics CSV files (e.g., system_metrics_HOSTNAME.csv).')
    args = parser.parse_args()

    if not os.path.isdir(args.metrics_directory):
        print(f"Error: The directory '{args.metrics_directory}' does not exist.")
        return

    csv_files = glob.glob(os.path.join(args.metrics_directory, 'system_metrics_*.csv'))
    
    if not csv_files:
        print(f"No system_metrics_*.csv files found in {args.metrics_directory}.")
        return

    total_plots_generated = 0
    for csv_filepath in csv_files:
        # Extract node identifier from filename, assuming format like "system_metrics_NODENAME.csv"
        # or "system_metrics_NODENAME_anything_else.csv"
        filename = os.path.basename(csv_filepath)
        if filename.startswith("system_metrics_") and filename.endswith(".csv"):
            # remove "system_metrics_" prefix and ".csv" suffix
            identifier_part = filename[len("system_metrics_"):-len(".csv")]
            # If there are other parts separated by underscore, take the first one as the primary node identifier
            node_identifier = identifier_part.split('_')[0] 
        else:
            node_identifier = "unknown_node" # Fallback
        
        print(f"Processing metrics for {node_identifier} from file: {csv_filepath}")
        # Pass the parent directory of the CSV files as the base for plot subdirectories
        total_plots_generated += plot_single_node_metrics(csv_filepath, node_identifier, args.metrics_directory)

    if total_plots_generated > 0:
        print(f"All plotting finished. Total plots generated: {total_plots_generated}.")
        print(f"Plots are saved in subdirectories within: {args.metrics_directory}")

if __name__ == '__main__':
    main()
