import csv
import asyncio
import os
import configparser
import shutil
import sys
import time
from collections import defaultdict
from rich.console import Console
from rich.table import Table
import json
import logging
from typing import List, Tuple, Dict, Any

csv_file: str = 'object_mapping.csv'
deploy_script: str = 'deploy_functions.py'
summary_json: str = 'deploy_summary.json'
console: Console = Console()

# --- Logging setup ---
LOG_FILE = 'deploy.log'
logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def run_query(source_object: str, target_object: str) -> None:
    """
    Executes the deploy_functions.py script as a subprocess for a given source and target object.
    Logs the process start, success, and errors with duration.
    """
    cmd: List[str] = [
        sys.executable,
        deploy_script,
        '-so', source_object,
        '-to', target_object
    ]
    start: float = time.time()
    logger.info(f"Starting deployment: {source_object} -> {target_object}")
    console.print(f"Starting deployment: {source_object} -> {target_object}")
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        # Echo the captured output to the console
        if stdout:
            print(stdout.decode())
        if stderr:
            print(stderr.decode())
        end: float = time.time()
        duration = end - start

        if process.returncode == 0:
            console.print(f"[green] ✅ {source_object} done in {duration:.1f}s[/green]")
            logger.info(f"Deployment succeeded for {source_object} in {duration:.1f}s")
        else:
            console.print(f"[red]❌ {source_object} failed in {duration:.1f}s[/red]")
            logger.error(f"Deployment failed for {source_object} in {duration:.1f}s, return code {process.returncode}")
            if stderr:
                error_msg = stderr.decode().strip()
                console.print(f"[red]{error_msg}[/red]")
                logger.error(f"Error details for {source_object}: {error_msg}")
            if stdout:
                info_msg = stdout.decode().strip()
                logger.error(f"Stdout for {source_object}: {info_msg}")
    except Exception as ex:
        logger.error(f"Exception running deploy for {source_object}: {ex}")
        console.print(f"[red]Exception running deploy for {source_object}: {ex}[/red]")

async def main() -> None:
    """
    Orchestrates deployment for each source-target object pair defined in the object_mapping CSV.
    Cleans up old summaries, executes each deploy as a subprocess, and prints a rich summary table.
    """
    mapping: List[Tuple[str, str]] = []

    # Clean old summary before starting new run
    if os.path.exists(summary_json):
        os.remove(summary_json)
        logger.info("Removed old deploy summary JSON.")

    # Read mapping CSV
    try:
        with open(csv_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                source_object = row.get('Source Org Object', '').strip()
                target_object = row.get('Target Org Object', '').strip()
                if source_object and target_object:
                    mapping.append((source_object, target_object))
                else:
                    msg = f"⚠️ Skipping row with missing object names: {row}"
                    print(msg)
                    logger.warning(msg)
    except Exception as ex:
        logger.error(f"Error reading object mapping CSV {csv_file}: {ex}")
        console.print(f"[red]Error reading object mapping CSV {csv_file}: {ex}[/red]")
        return

    for source_object, target_object in mapping:
        await run_query(source_object, target_object)

    # Print summary table
    if os.path.exists(summary_json):
        try:
            with open(summary_json, 'r') as f:
                summary: Dict[str, Dict[str, Any]] = json.load(f)
            table: Table = Table(title="Deploy Error/Info Summary", show_lines=True)
            table.add_column("Object")
            table.add_column("Type")
            table.add_column("Count", justify="right")
            for obj in sorted(summary.keys()):
                for category in sorted(summary[obj].keys()):
                    table.add_row(obj, category, str(summary[obj][category]))
            console.print(table)
            logger.info("Printed deploy summary table.")
        except Exception as ex:
            logger.error(f"Error reading/parsing {summary_json}: {ex}")
            print(f"[red]Error reading or parsing deploy summary: {ex}[/red]")
    else:
        print("No deploy summary found.")
        logger.warning("No deploy summary JSON file was found after deployment.")

    print("\n✅ All object migrations are complete. See deploy.log for details.")

if __name__ == "__main__":
    asyncio.run(main())
