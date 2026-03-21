import csv
import asyncio
import os
import sys
import configparser
import shutil
import logging
import argparse
import subprocess
from typing import Dict, Any, List, Tuple

from rich.table import Table
from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, SpinnerColumn, TextColumn

csv_file = 'object_mapping.csv'
download_script = 'download_functions.py'
config_path = 'config.ini'

# --- Granular logging setup ---
LOG_FILE = 'download.log'
logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = config_path) -> configparser.ConfigParser:
    """
    Loads the configuration file.
    """
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(config_path)
    return config


def preflight_checks(mode: str = 'batch') -> None:
    """
    Ensures all necessary files and directories exist and are readable.
    """
    errors = []
    if not os.path.isfile(download_script):
        errors.append(f"Download script not found: {download_script}")
    if mode == 'batch' and not os.path.isfile(csv_file):
        errors.append(f"Object mapping CSV not found: {csv_file}")
    if not os.path.isfile(config_path):
        errors.append(f"Config file not found: {config_path}")
    if errors:
        for e in errors:
            print(f"[red]{e}[/red]")
            logger.error(e)
        sys.exit(1)


async def run_query(
        source_object: str,
        progress_task_id: int,
        progress,
        error_log: List[str],
        extra_params: List[str],
) -> Tuple[str, str, str]:
    """
    Runs the download script as a subprocess for a given Salesforce object.
    Updates progress and error log as appropriate.
    Returns a tuple: (object, status, message)
    """
    query = f"SELECT Id FROM {source_object}"
    cmd = [
              sys.executable,
              download_script,
              '-q', query,
              '-so', source_object
          ] + extra_params

    logger.info(f"Prepared command: {' '.join(cmd)}")

    progress.console.print(f":rocket: [cyan]Running:[/cyan] {' '.join(cmd)}")

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    # Logging both stdout and stderr
    if stdout:
        logger.info(f"[{source_object}] STDOUT: {stdout.decode().strip()}")
    if stderr:
        logger.error(f"[{source_object}] STDERR: {stderr.decode().strip()}")

    progress.update(progress_task_id, advance=1)

    if process.returncode == 0:
        msg = stdout.decode().strip()
        progress.console.print(f":white_check_mark: [green]{source_object} succeeded.[/green]")
        return (source_object, "Success", msg)
    else:
        msg = stderr.decode().strip()
        error_message = f"{source_object}: {msg}"
        error_log.append(error_message)
        progress.console.print(f":x: [red]{source_object} failed[/red]: {msg}")
        return (source_object, "Failed", msg)


def prepare_output_directory(config: configparser.ConfigParser) -> None:
    """
    Prepares (and if configured, clears) the output directory for file exports.
    Prompts the user before deletion unless auto-delete is enabled.
    """
    output_directory: str = config['salesforce']['output_dir']
    output_dir_auto_delete: str = config['salesforce'].get('output_dir_auto_delete', 'false').lower()

    # Check if output directory exists and is a directory
    if os.path.exists(output_directory) and os.path.isdir(output_directory):
        items_to_delete: List[str] = [os.path.join(output_directory, item) for item in os.listdir(output_directory)]

        if items_to_delete:
            print("The following files and directories will be deleted:\n")
            for path in items_to_delete:
                print(f"  - {path}")

            should_delete = False
            if output_dir_auto_delete == 'true':
                should_delete = True
            else:
                confirm = input("\nAre you sure you want to delete these items? (yes/no): ").strip().lower()
                if confirm in ('y', 'yes'):
                    should_delete = True
                else:
                    print("\nDeletion cancelled.")

            if should_delete:
                for path in items_to_delete:
                    if os.path.isfile(path) or os.path.islink(path):
                        os.remove(path)
                    elif os.path.isdir(path):
                        shutil.rmtree(path)
                print(f"\nCleared contents of: {output_directory}")
        else:
            print(f"No files or directories to delete in: {output_directory}")
    else:
        print(f"Directory does not exist: {output_directory}")

    # Create output directory if it does not exist
    if not os.path.isdir(output_directory):
        os.mkdir(output_directory)
        print(f"Created output directory: {output_directory}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Salesforce files/attachments. Supports batch mode (CSV) or CLI mode (direct query).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Batch mode (default) - uses object_mapping.csv
  python download.py

  # CLI mode - download files from specific Account records
  python download.py --mode cli -q "SELECT Id FROM Account WHERE Industry='Tech'" -so Account

  # CLI mode with file extension filter (Excel files only)
  python download.py --mode cli -q "SELECT Id FROM Case" -so Case -fe "'EXCEL_M', 'EXCEL'"

  # CLI mode with custom filename pattern (linked entity name in path)
  python download.py --mode cli -q "SELECT Id FROM Account" -so Account -f "{0}{4}/{1}-{2}.{3}"

  # Download + immediate deploy
  python download.py --mode cli -q "SELECT Id FROM Case LIMIT 10" -so Case --deploy
        """
    )
    parser.add_argument("--mode", choices=['batch', 'cli'], default='batch',
                        help="Operation mode: 'batch' uses object_mapping.csv, 'cli' uses direct SOQL query (default: batch)")
    parser.add_argument("-q", "--query", metavar='QUERY',
                        help="SOQL query to limit records (required for CLI mode)")
    parser.add_argument("-so", "--source-object", metavar='OBJECT',
                        help="Source object name (required for CLI mode)")
    parser.add_argument("-o", "--object", metavar='TYPE', default='ContentDocumentLink',
                        help="How ContentDocuments are selected: 'ContentDocumentLink' (default) or 'ContentDocument'")
    parser.add_argument("-f", "--filenamepattern", metavar='PATTERN', default='{0}{1}-{2}.{3}',
                        help="Filename pattern: {0}=output_dir, {1}=content_doc_id, {2}=title, {3}=extension, {4}=linked_entity_name, {5}=version_number")
    parser.add_argument("-fe", "--filter_file_extension", metavar='FILTER', default=None,
                        help="Filter by file type, e.g. \"'EXCEL_M', 'EXCEL'\" (Salesforce FileType values)")
    parser.add_argument("--deploy", action='store_true',
                        help="Run deploy automatically after download completes")
    parser.add_argument("--extra", nargs=argparse.REMAINDER,
                        help="Extra parameters to pass through to download_functions.py (batch mode only, after '--extra')",
                        default=[])
    return parser.parse_args()


def run_cli_mode(args, config: configparser.ConfigParser) -> bool:
    """
    Run CLI mode - execute a single SOQL query download with Rich UI.
    Returns True on success, False on failure.
    """
    console = Console()

    # Build header panel showing query configuration
    config_items = [
        f"[cyan]Source Object:[/cyan] {args.source_object}",
        f"[cyan]Query:[/cyan] {args.query}",
        f"[cyan]Object Type:[/cyan] {args.object}",
        f"[cyan]Filename Pattern:[/cyan] {args.filenamepattern}",
    ]
    if args.filter_file_extension:
        config_items.append(f"[cyan]File Extension Filter:[/cyan] {args.filter_file_extension}")

    header_panel = Panel(
        "\n".join(config_items),
        title="[bold blue]CLI Mode Configuration[/bold blue]",
        expand=False
    )
    console.print(header_panel)
    console.print()

    # Build command
    cmd = [
        sys.executable,
        download_script,
        '-q', args.query,
        '-so', args.source_object,
        '-o', args.object,
        '-f', args.filenamepattern,
    ]
    if args.filter_file_extension:
        cmd.extend(['-fe', args.filter_file_extension])
    logger.info(f"CLI mode command: {' '.join(cmd)}")
    console.print(f":rocket: [cyan]Running:[/cyan] {' '.join(cmd)}")
    console.print()

    # Execute with direct terminal access (needed for Rich progress bar)
    process = subprocess.Popen(cmd)
    process.wait()

    if process.returncode == 0:
        console.print()
        console.print(Panel(
            f"[green]Download completed successfully for {args.source_object}[/green]",
            title="[bold green]Success[/bold green]",
            expand=False
        ))
        logger.info(f"CLI mode completed successfully for {args.source_object}")
        return True
    else:
        console.print()
        console.print(Panel(
            f"[red]Download failed for {args.source_object}[/red]",
            title="[bold red]Failed[/bold red]",
            expand=False
        ))
        logger.error(f"CLI mode failed for {args.source_object}")
        return False


async def run_batch_mode(args, config: configparser.ConfigParser) -> None:
    """
    Run batch mode - process objects from object_mapping.csv with Rich UI.
    """
    console = Console()
    extra_params = args.extra

    logger.info("Starting batch mode with extra_params=%s", extra_params)

    tasks: List[Any] = []
    object_names: List[str] = []
    error_log: List[str] = []

    # Read object mapping from CSV and create download tasks
    with open(csv_file, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            source_object = row.get('Source Org Object', '').strip()
            if source_object:
                object_names.append(source_object)
            else:
                console.print("[yellow]Skipping row with empty 'Source Org Object'[/yellow]")

    total_objects = len(object_names)

    if total_objects == 0:
        console.print("[red]No objects found to process. Exiting.[/red]")
        sys.exit(1)

    # Set up the progress bar UI and error panel
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=True  # Remove bar after done
    )

    # Start progress bar and process all downloads
    with progress:
        task_id = progress.add_task("[blue]Downloading objects...", total=total_objects)
        coros = [
            run_query(obj, task_id, progress, error_log, extra_params)
            for obj in object_names
        ]
        results = await asyncio.gather(*coros)

    # Print summary table using rich
    table = Table(title="Migration Summary", show_lines=True)
    table.add_column("Object", style="cyan", no_wrap=True)
    table.add_column("Status", style="green", justify="center")
    table.add_column("Message", style="magenta")

    for obj, status, message in results:
        msg_short = message.replace('\n', ' ')
        if len(msg_short) > 60:
            msg_short = msg_short[:57] + "..."
        if status == "Success":
            status_style = "[green]Success[/green]"
        else:
            status_style = "[red]Failed[/red]"
        table.add_row(obj, status_style, msg_short)

    total = len(results)
    succeeded = sum(1 for r in results if r[1] == "Success")
    failed = sum(1 for r in results if r[1] == "Failed")

    stats_panel = Panel(
        f"[bold]Total:[/bold] {total}    [green]Succeeded:[/green] {succeeded}    [red]Failed:[/red] {failed}",
        title="[yellow]Migration Results[/yellow]", expand=False
    )

    # Show error log panel if there are errors
    error_panel = None
    if error_log:
        error_text = "\n".join([f"[red]{err}[/red]" for err in error_log])
        error_panel = Panel(error_text, title="[bold red]Errors Encountered[/bold red]", expand=False)

    group_items = [table, stats_panel]
    if error_panel:
        group_items.append(error_panel)

    group = Group(*group_items)
    console.print("\n")
    console.print(group)

    logger.info("Batch mode completed: Success=%s, Failed=%s", succeeded, failed)


def run_deploy(source_object: str = None) -> bool:
    """
    Run the deploy script after download.
    If source_object is provided, only deploy that object.
    Returns True on success, False on failure.
    """
    console = Console()
    deploy_script = 'deploy.py'

    if not os.path.isfile(deploy_script):
        console.print(f"[red]Deploy script not found: {deploy_script}[/red]")
        return False

    console.print()
    console.print(Panel(
        "[cyan]Starting deployment...[/cyan]",
        title="[bold blue]Deploy Phase[/bold blue]",
        expand=False
    ))

    cmd = [sys.executable, deploy_script]
    logger.info(f"Running deploy: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        console.print(line.rstrip())

    process.wait()

    if process.returncode == 0:
        logger.info("Deploy completed successfully")
        return True
    else:
        logger.error("Deploy failed")
        return False


async def main() -> None:
    """
    Main entry point - determines mode and executes appropriate workflow.
    """
    args = parse_args()
    console = Console()

    logger.info("Starting download script in %s mode", args.mode)

    # Validate CLI mode requirements
    if args.mode == 'cli':
        if not args.query:
            console.print("[red]Error: --query (-q) is required for CLI mode[/red]")
            sys.exit(1)
        if not args.source_object:
            console.print("[red]Error: --source-object (-so) is required for CLI mode[/red]")
            sys.exit(1)

    preflight_checks(mode=args.mode)
    config = load_config()

    if args.mode == 'batch':
        prepare_output_directory(config)
        await run_batch_mode(args, config)
    else:  # CLI mode
        success = run_cli_mode(args, config)
        if args.deploy and success:
            run_deploy(args.source_object)

    logger.info("Script completed")


if __name__ == "__main__":
    asyncio.run(main())
