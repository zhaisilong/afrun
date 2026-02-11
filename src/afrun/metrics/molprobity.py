import subprocess
from .metrics import Metrics
from pathlib import Path
import re
from loguru import logger
from afcp.utils.results_utils import Results
import os


def extract_molprobity_results(results_file: str) -> dict:
    """
    Extract the results from the molprobity output file.
    """
    try:
        results_path = Path(results_file)
        results_txt = results_path.read_text()
        results = {
            "Ramachandran outliers": None,
            "favored": None,
            "Rotamer outliers": None,
            "C-beta deviations": None,
            "Clashscore": None,
            "RMS(bonds)": None,
            "RMS(angles)": None,
            "MolProbity score": None,
        }
        summary_pattern = r"=+ Summary =+\n\n(.*)$"
        summary_match = re.search(summary_pattern, results_txt, re.DOTALL)
        if summary_match:
            summary_txt = summary_match.group(1)
            for line in summary_txt.split("\n"):
                if "=" in line:
                    key, value = line.split("=")
                    results.update({key.strip(): value.replace("%", "").strip()})
            return results
    except Exception as e:
        logger.error(f"Error extracting molprobity results from {results_file}: {e}")
        return results


class MolProbity(object):
    def __init__(self, script_path: Path | None = None):
        self.script_path = (
            Path(__file__).parents[1] / "scripts" / "run_molprobity.sh"
            if script_path is None
            else script_path
        )
        assert (
            self.script_path.exists()
        ), f"MolProbity script not found at {self.script_path}"
        assert os.access(
            self.script_path, os.X_OK
        ), f"MolProbity script is not executable at {self.script_path}; please make it executable with `chmod +x {self.script_path}`"

    def __call__(self, prot_path: Path):
        cache_dir = prot_path.parent / ".cache" / "molprobity"
        cache_dir.mkdir(parents=True, exist_ok=True)
        prefix = prot_path.stem
        finished_file = cache_dir / f"molprobity.done"
        results_file = cache_dir / f"{prefix}.out"
        if finished_file.exists() and results_file.exists():
            logger.debug(f"MolProbity already finished for {prot_path}")
            return extract_molprobity_results(results_file)
        # cmd script, cache_dir, prot_path, prefix
        cmd = f"{self.script_path} {cache_dir.resolve()} {prot_path.resolve()} {prefix}"
        logger.debug(f"Running MolProbity with command: {cmd}")
        cmd_out = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
        )
        if cmd_out.returncode != 0:
            logger.error(f"MolProbity failed with return code {cmd_out.returncode}")
            raise ValueError(f"MolProbity failed with return code {cmd_out.returncode}")
        results = extract_molprobity_results(results_file)
        return results
