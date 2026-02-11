import click
import shutil
from pathlib import Path
from loguru import logger
import random
from tqdm import tqdm
import json
from itertools import product

from .models.af3 import AF3, DEFAULT_BASH_PATH, DEFAULT_BASH_DEV_PATH
from .evaluator import RMSDEvaluator
from .utils.gpu_utils import GPUQueue
from .utils.results_utils import Results
from .msa.msadb import MSADB, DEFAULT_DB_PATH

from akasha.pdb import download_pdb

from afrun.utils.sturucture_utils import split_mmcif_to_models, PDBPreparer
from afrun.utils.config_utils import load_json
from afrun.utils.sturucture_utils import cif2pdb
from afrun.evaluator.rmsd import RMSDMonomer, RMSDComplex


@click.group()
def cli():
    pass


def common_options(func):
    for option in reversed(
        [
            click.option("--data_dir", default="./afdbcp", help="Working directory"),
            click.option("--job_id", default=None, help="Job ID to run"),
            click.option("--gpu_id", default=3, type=int, help="GPU ID to use"),
            click.option(
                "--model_name", default="af3", type=str, help="Model name to use"
            ),
        ]
    ):
        func = option(func)
    return func


@cli.command("predict")
@common_options
@click.option(
    "--mode",
    type=click.Choice(["full", "msa", "infer"]),
    default="full",
    help="Runing mode",
)
@click.option(
    "--shuffle",
    is_flag=True,
    help="Shuffle the data",
)
@click.option(
    "--dev",
    is_flag=True,
    help="Use dev model",
)
def predict(
    data_dir: Path,
    job_id: str,
    gpu_id: int,
    mode: str,
    shuffle: bool,
    model_name: str,
    dev: bool,
):
    if dev:
        bash_script = DEFAULT_BASH_DEV_PATH
    else:
        bash_script = DEFAULT_BASH_PATH

    if not bash_script.exists():
        raise FileNotFoundError(f"Bash script not found: {bash_script}")

    if model_name == "af3":
        model = AF3(
            bash_path=bash_script,
            gpu_pool=GPUQueue(gpus=[gpu_id]),
        )
    else:
        raise ValueError(f"Unknown model name: {model_name}")

    if job_id:
        logger.info(f"Running single job {job_id}")
        job_dir = Path(data_dir) / job_id
        job_model_dir = job_dir / model_name
        model(
            job_name=job_id,
            model_job_dir=job_model_dir,
            mode=mode,
            verbose=True,
        )
    else:
        logger.info(f"Running all jobs in {data_dir}")
        jobs = [p for p in Path(data_dir).glob("*") if p.is_dir()]
        if shuffle:
            logger.info(f"Shuffling jobs")
            random.shuffle(jobs)
        for job in tqdm(jobs, desc="Running jobs", total=len(jobs)):
            job_id = job.name
            job_model_dir = job / model_name
            logger.info(f"Running job {job_id}")
            model(
                job_name=job_id,
                model_job_dir=job_model_dir,
                mode=mode,
                verbose=True,
            )


def iter_af3_dir(model_preds_dir: Path):
    for model_dir in model_preds_dir.glob(f"seed*sample*"):
        if model_dir.is_dir():
            seed_id = int(model_dir.stem.split("_")[0].split("seed-")[1])
            sample_id = int(model_dir.stem.split("_")[1].split("sample-")[1])
            pred_path, conf_path, summary_path = None, None, None
            for file in model_dir.iterdir():
                if file.suffix == ".cif":
                    pred_path = file
                elif "_summary" in file.stem:  # 要在 _confidences 之前
                    summary_path = file
                elif "_confidences" in file.stem:
                    conf_path = file
            assert (
                pred_path is not None
                and conf_path is not None
                and summary_path is not None
            )
            yield (seed_id, sample_id, pred_path, conf_path, summary_path)


def preprocess_single(job_id: str, job_dir: Path, model_name: str):
    model_job_dir = job_dir / f"{model_name}"
    processed_job_dir = job_dir / f"{model_name}_processed"

    pre_done_flag = processed_job_dir / "pre.done"
    if pre_done_flag.exists():
        logger.warning(f"Pre-processing for {job_id} already exists, skipping")
        return

    # 处理参考结构
    local_pdb_path = job_dir / f"{job_id}.cif"
    if not local_pdb_path.exists():
        pdb_path = download_pdb(job_id, processed_job_dir, force_write=False)
    else:
        pdb_path = local_pdb_path

    refer_paths = split_mmcif_to_models(
        cif_file=pdb_path,
        output_dir=processed_job_dir / "refers",
        force_write=False,
    )
    pdb_preparer = PDBPreparer(force_write=False)
    with Results(
        results_dir=job_dir / "results",
        file_name="refers.csv",
        force_write=False,
    ) as refer_results:
        for refer_path in refer_paths:
            refer_path_ = pdb_preparer.prepare_pdb(
                refer_path,
                chain_map=None,
                verbose=False,
                is_monomer=False,
            )
            refer_results.add(
                {
                    "job_id": job_id,
                    "refer_path": str(refer_path_),
                }
            )
    # 处理预测结构
    with Results(
        job_dir / "results",
        "af3_preds.csv",
        force_write=True,
    ) as af_results:
        preds_dir = processed_job_dir / "preds"
        preds_dir.mkdir(parents=True, exist_ok=True)

        for seed_id, sample_id, pred_path, conf_path, summary_path in iter_af3_dir(
            model_job_dir / f"{job_id.lower()}"
        ):
            summary_content = json.loads(summary_path.read_text())

            pred_model_dir = preds_dir / f"{seed_id}_{sample_id}"
            pred_model_dir.mkdir(parents=True, exist_ok=True)

            pred_model_path = pred_model_dir / "struct.cif"
            if not pred_model_path.exists():
                shutil.copy(pred_path, pred_model_path)

            pred_model_path_ = cif2pdb(pred_model_path, force_write=False)
            pred_model_path__ = pdb_preparer.prepare_pdb(
                pred_model_path_, verbose=False
            )
            af_results.add(
                {
                    "job_id": job_id,
                    "seed_id": seed_id,
                    "sample_id": sample_id,
                    "pred_path": str(pred_model_path__),
                    "iptm": summary_content["iptm"],
                    "ptm": summary_content["ptm"],
                    "ranking_score": summary_content["ranking_score"],
                    "fraction_disordered": summary_content["fraction_disordered"],
                    "has_clash": summary_content["has_clash"],
                }
            )
    pre_done_flag.touch()


@cli.command("preprocess")
@common_options
def preprocess(data_dir: Path, job_id: str, model_name: str, gpu_id: int):
    if job_id:
        logger.info(f"Preprocessing single job {job_id}")
        job_dir = Path(data_dir) / job_id
        preprocess_single(job_id, job_dir, model_name)
    else:
        logger.info(f"Preprocessing all jobs in {data_dir}")
        jobs = [p for p in Path(data_dir).glob("*") if p.is_dir()]
        for job in tqdm(jobs, desc="Preprocessing jobs", total=len(jobs)):
            job_id = job.name
            job_dir = Path(data_dir) / job_id
            preprocess_single(job_id, job_dir, model_name)


def evaluate_single(job_id: str, job_dir: Path, model_name: str):
    processed_job_dir = job_dir / f"{model_name}_processed"
    assert (
        processed_job_dir / "pre.done"
    ).exists(), f"Pre-processing for {job_id} not finished"
    eval_config = load_json(job_dir / "eval.json")

    refer_results = Results(
        job_dir / "results",
        "refers.csv",
    )
    af3_results = Results(
        job_dir / "results",
        "af3_preds.csv",
    )
    refer_results.load()
    af3_results.load()

    if eval_config["is_monomer"]:
        rmsd = RMSDMonomer()
    else:
        rmsd = RMSDComplex()

    with Results(
        processed_job_dir / "results",
        "rmsd.csv",
        force_write=True,
    ) as rmsd_results:
        for refer_record, af3_record in product(
            refer_results.get_all(), af3_results.get_all()
        ):
            refer_path = refer_record["refer_path"]
            pred_path = af3_record["pred_path"]
            _results = rmsd(
                refer_path,
                pred_path,
                eval_config["protein_chains"],
                eval_config["peptide_chains"],
            )
            _results.update(
                {
                    "refer_path": refer_record["refer_path"],
                    "pred_model_path": af3_record["pred_model_path"],
                }
            )
            rmsd_results.add(_results)


@cli.command("eval")
@common_options
def evaluate(data_dir: Path, job_id: str, model_name: str, gpu_id: int):
    if job_id:
        logger.info(f"Evaluating single job {job_id}")
        job_dir = Path(data_dir) / job_id
        evaluate_single(job_id, job_dir, model_name)
    else:
        logger.info(f"Evaluating all jobs in {data_dir}")
        jobs = [p for p in Path(data_dir).glob("*") if p.is_dir()]
        for job in tqdm(jobs, desc="Evaluating jobs", total=len(jobs)):
            job_id = job.name
            job_dir = Path(data_dir) / job_id
            evaluate_single(job_id, job_dir, model_name)


@cli.group("msa")
@click.option(
    "--db_path",
    default=DEFAULT_DB_PATH,
    help="MSA database path",
)
@click.pass_context
def msa(ctx: click.Context, db_path: Path):
    """
    Operations for the MSA database.

    用法示例:
      afrun msa status --db_path ./msa.db
      afrun msa add data.json --db_path ./msa.db
      afrun msa list --db_path ./msa.db
      afrun msa query KEY --db_path ./msa.db
      afrun msa export output.json --db_path ./msa.db
    """
    ctx.ensure_object(dict)
    db_path = Path(db_path)
    ctx.obj["db_path"] = db_path
    ctx.obj["db"] = MSADB(db_path)


@msa.command("status")
@click.pass_context
def msa_status(ctx: click.Context):
    """Show database status"""
    try:
        db: MSADB = ctx.obj["db"]
        status = db.status()
        logger.info(f"\nMSA database status:\n{status}")
    finally:
        db.close()


@msa.command("add")
@click.argument("import_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--force",
    is_flag=True,
    help="Force overwrite the existing sequence",
)
@click.pass_context
def msa_add(ctx: click.Context, import_path: Path, force: bool):
    """Import a JSON file into the MSA database"""
    try:
        db: MSADB = ctx.obj["db"]
        import_path = Path(import_path)
        seq_id = db.import_json_file(import_path, force=force)
        logger.success(f"Imported sequence with id: {seq_id}")
    finally:
        db.close()


@msa.command("list")
@click.pass_context
def msa_list(ctx: click.Context):
    """List all sequences in the MSA database"""
    try:
        db: MSADB = ctx.obj["db"]
        sequences = db.list()
        logger.info(f"Sequences in the MSA database: {sequences}")
    finally:
        db.close()


@msa.command("update")
@click.argument("json_path", type=click.Path(exists=True, path_type=Path))
@click.option("--output_path", type=click.Path(path_type=Path), help="Output path", default=None)
@click.pass_context
def msa_update(ctx: click.Context, json_path: Path, output_path: Path | None = None):
    """Update the MSA database with a JSON file"""
    try:
        json_path = Path(json_path)
        output_path = json_path.with_name(json_path.stem + "_data.json") if not output_path else Path(output_path)
        
        db: MSADB = ctx.obj["db"]
        data = db.update_json_file(json_path)
        output_path.write_text(json.dumps(data, indent=2))

        logger.success(f"Updated MSA database with {output_path}")
    finally:
        db.close()


@msa.command("search")
@click.argument("query", type=str)
@click.option(
    "--limit",
    type=int,
    default=50,
    help="Limit the number of sequences to return",
)
@click.pass_context
def msa_search(ctx: click.Context, query: str, limit: int):
    """Search for sequences in the MSA database"""
    try:
        db: MSADB = ctx.obj["db"]
        sequences = db.search(query, limit=limit)
        logger.info(f"Sequences found: {sequences}")
    finally:
        db.close()


if __name__ == "__main__":
    cli()
