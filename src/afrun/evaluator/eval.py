import asyncio
import subprocess
import sys
from pathlib import Path
from loguru import logger


class Evaluator:
    def __init__(self):
        pass

    def manage_status_files(self, model_job_dir, mode, status):
        status_file = model_job_dir / f"{mode}.{status}"
        running_file = model_job_dir / f"{mode}.running"
        error_file = model_job_dir / f"{mode}.error"

        if status == "running":
            running_file.touch()
            logger.info(f"Created running file: {running_file}")
        elif status == "done":
            if running_file.exists():
                running_file.unlink()
                logger.info(f"Deleted running file: {running_file}")
            if error_file.exists():
                error_file.unlink()
                logger.info(f"Deleted error file: {error_file}")
            status_file.touch()
            logger.info(f"Created done file: {status_file}")

        elif status == "error":
            if running_file.exists():
                running_file.unlink()
                logger.info(f"Deleted running file: {running_file}")
            error_file.touch()
            logger.info(f"Created error file: {error_file}")

    def check_status(self, model_job_dir, mode):
        """检查状态文件"""
        done_file = model_job_dir / f"{mode}.done"
        running_files = [model_job_dir / f"{mode}.running"]
        error_file = model_job_dir / f"{mode}.error"

        preprocess_done = model_job_dir / f"preprocess.done"

        # 检查是否有任何 .running 文件
        for running_file in running_files:
            if running_file.exists():
                logger.info(
                    f"{mode}: A task is currently running ({running_file.name}). Skipping."
                )
                return "running"

        # 检查当前模式的 .done 文件
        if done_file.exists():
            logger.info(f"{mode} already completed. Skipping.")
            return "done"

        if mode == "evaluate" and not preprocess_done.exists():
            logger.info(f"{mode} requires preprocess first")
            return "error"

        # 检查当前模式的 .error 文件
        if error_file.exists():
            logger.info(f"{mode} encountered an error previously. Retrying.")
            return "error"

        # 如果没有任何标记文件，任务为 pending
        return "pending"

    async def run_mode(
        self,
        job_id: str,
        job_dir: Path,
        mode: str,
        verbose: bool = False,
    ):
        status = self.check_status(job_dir, mode)
        if status in ["done", "running"]:
            return  # 跳过已完成或正在运行的任务
        try:
            self.manage_status_files(job_dir, mode, "running")
            cmd = await self.generate_cmd(
                job_id, job_dir, mode == "preprocess", mode == "evaluate"
            )
            if verbose:
                logger.info(f"Running command: {cmd}")
            subprocess.run(cmd, check=True)
            self.manage_status_files(job_dir, mode, "done")
            logger.info(f"{mode} completed successfully.")

        except subprocess.CalledProcessError as e:
            self.manage_status_files(job_dir, mode, "error")
            logger.error(f"Error: {mode} encountered an issue. {e}")
            sys.exit(1)
        except KeyboardInterrupt:
            self.manage_status_files(job_dir, mode, "error")
            logger.error(f"Run interrupted by user during {mode}.")
            sys.exit(1)
        except Exception as e:
            self.manage_status_files(job_dir, mode, "error")
            logger.error(f"Error: {mode} encountered an unexpected issue. {e}")
            sys.exit(1)

    def __call__(
        self,
        job_id: str,
        job_dir: Path,
        mode: str = "full",
        verbose: bool = False,
    ):
        if mode == "full":
            asyncio.run(self.run_mode(job_id, job_dir, "preprocess", verbose))
            asyncio.run(self.run_mode(job_id, job_dir, "evaluate", verbose))
        else:
            asyncio.run(self.run_mode(job_id, job_dir, mode, verbose))

    def generate_cmd(self, job_id: str, job_dir: Path, mode: str):
        raise NotImplementedError("Subclasses must implement this method")


class RMSDEvaluator(Evaluator):
    def __init__(self):
        super().__init__()

    def generate_cmd(self, job_id: str, job_dir: Path, mode: str):
        pass


# class AF3Job:
#     def __init__(
#         self,
#         job_dir: str,
#         job_id: Optional[str] = None,
#     ):
#         self.job_dir = Path(job_dir)
#         if job_id is None:
#             self.job_id = self.job_dir.name
#         else:
#             self.job_id = job_id

#     @property
#     def status(self):
#         msa_flag_path = self.job_dir / "msa.done"
#         infer_flag_path = self.job_dir / "infer.done"
#         if msa_flag_path.exists() and infer_flag_path.exists():
#             return "finished"
#         else:
#             return "unfinished"

#     @classmethod
#     def iter_dir(
#         cls, dir: Path
#     ) -> Generator[Tuple[int, int, Path, Path, Path], None, None]:
#         for model_dir in dir.glob(f"seed*sample*"):
#             if model_dir.is_dir():
#                 seed_id = int(model_dir.stem.split("_")[0].split("seed-")[1])
#                 sample_id = int(model_dir.stem.split("_")[1].split("sample-")[1])
#                 pred_path, conf_path, summary_path = None, None, None
#                 for file in model_dir.iterdir():
#                     if file.suffix == ".cif":
#                         pred_path = file
#                     elif "_summary" in file.stem:  # 要在 _confidences 之前
#                         summary_path = file
#                     elif "_confidences" in file.stem:
#                         conf_path = file
#                 assert (
#                     pred_path is not None
#                     and conf_path is not None
#                     and summary_path is not None
#                 )
#                 yield (seed_id, sample_id, pred_path, conf_path, summary_path)

#     def pre_eval(self, force_write: bool = False):
#         self.eval_dir = self.job_dir / self.job_id / "eval"
#         self.eval_dir.mkdir(parents=True, exist_ok=True)
#         pre_eval_flag = self.eval_dir / "pre_eval.done"
#         if pre_eval_flag.exists() and not force_write:
#             logger.warning(f"Pre-evaluation for {self.cp_id} already exists, skipping")
#             return

#         pdb_id = self.metadata["pdb_id"]

#         # 1. fetch reference pdb files
#         pdb_path = download_pdb(pdb_id, self.eval_dir, force_write)

#         # 2. prepare pdb files
#         refer_paths = split_mmcif_to_models(
#             cif_file=pdb_path,
#             output_dir=self.eval_dir / "refers",
#             force_write=self.force_write,
#         )
#         logger.debug(f"refer_paths: {refer_paths}")
#         pdb_preparer = PDBPreparer(force_write=self.force_write)
#         with Results(
#             self.eval_dir.parent / "results",
#             "refers.csv",
#             force_write=self.force_write,
#         ) as refer_results:
#             for refer_path in refer_paths:
#                 chain_map = self.build_chain_map(
#                     self.metadata["pdb_peptide_chains"],
#                     self.metadata["peptide_chains"],
#                     self.metadata["pdb_protein_chains"],
#                     self.metadata["protein_chains"],
#                 )
#                 logger.debug(f"chain_map: {chain_map}")
#                 refer_path_ = pdb_preparer.prepare_pdb(
#                     refer_path,
#                     chain_map=chain_map,
#                     verbose=False,
#                     is_monomer=self.metadata["is_monomer"],
#                 )
#                 refer_results.add(
#                     {
#                         "cp_id": self.cp_id,
#                         "pdb_id": pdb_id,
#                         "refer_path": refer_path_,
#                     }
#                 )
#         # 3. gather predicted pdb files
#         model_dir = self.af3_job_dir / self.cp_id.lower()
#         # 3.1 顺带收集一些数据
#         with Results(
#             self.eval_dir.parent / "results",
#             "af3_preds.csv",
#             force_write=self.force_write,
#         ) as af_results:
#             preds_dir = self.eval_dir / "preds"
#             preds_dir.mkdir(parents=True, exist_ok=True)

#             for (
#                 seed_id,
#                 sample_id,
#                 pred_path,
#                 conf_path,
#                 summary_path,
#             ) in self.iter_af3_dir(model_dir):
#                 summary_content = json.loads(summary_path.read_text())
#                 pred_model_dir = preds_dir / f"{seed_id}_{sample_id}"
#                 pred_model_dir.mkdir(parents=True, exist_ok=True)
#                 pred_model_path = pred_model_dir / "struct.cif"

#                 if not pred_model_path.exists() or self.force_write:
#                     shutil.copy(pred_path, pred_model_path)

#                 # *.cif -> *.pdb
#                 pred_model_path_ = cif2pdb(
#                     pred_model_path, force_write=self.force_write
#                 )
#                 pred_model_path__ = pdb_preparer.prepare_pdb(
#                     pred_model_path_, verbose=False
#                 )
#                 af_results.add(
#                     {
#                         "cp_id": self.cp_id,
#                         "seed_id": seed_id,
#                         "sample_id": sample_id,
#                         "pred_path": pred_path,
#                         "pred_model_path": pred_model_path__,
#                         "iptm": summary_content["iptm"],
#                         "ptm": summary_content["ptm"],
#                         "ranking_score": summary_content["ranking_score"],
#                         "fraction_disordered": summary_content["fraction_disordered"],
#                         "has_clash": summary_content["has_clash"],
#                     }
#                 )

#         # 4. prepare predicted pdb files
#         pre_eval_flag.touch()
