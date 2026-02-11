from pathlib import Path

from ._base import BaseModel
from ..utils.gpu_utils import GPUQueue

DEFAULT_BASH_PATH = Path(__file__).parent.parent / "scripts/run_alphafold.sh"
DEFAULT_BASH_DEV_PATH = Path(__file__).parent.parent / "scripts/run_alphafold_dev.sh"


class AF3(BaseModel):
    def __init__(self, gpu_pool: GPUQueue, bash_path: Path = DEFAULT_BASH_PATH):

        super().__init__(gpu_pool, bash_path)

    async def generate_cmd(
        self,
        job_name: str,
        model_job_dir: Path,
        run_data_pipeline: bool,
        run_inference: bool,
    ):
        assert (
            run_data_pipeline or run_inference
        ), "At least one of run_data_pipeline or run_inference must be True"
        if run_inference:
            input_json_path = model_job_dir / job_name / f"{job_name}_data.json"
        else:
            input_json_path = model_job_dir / f"{job_name}.json"

        self.gpu_id = await self.gpu_pool.acquire_gpu(min_free_memory=60000)
        if self.gpu_id is not None:
            print(f"Using GPU {self.gpu_id} for computation.")

        cmd = [
            "bash",
            str(self.bash_path.resolve()),
            str(input_json_path.resolve()),
            str(model_job_dir.resolve()),
            str(run_data_pipeline).lower(),
            str(run_inference).lower(),
            str(self.gpu_id),
        ]

        return cmd
