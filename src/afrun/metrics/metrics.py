from pathlib import Path
from typing import List


class Metrics(object):
    def __init__(
        self,
        prot_path: List[Path] | Path | None = None,
        pep_path: List[Path] | Path | None = None,
        rec_path: List[Path] | Path | None = None,
        cache_dir: Path | None = None,
        script_path: Path | None = None,
        results_dir: Path | None = None,
        *kwargs,
    ):
        self.prot_path = prot_path
        self.pep_path = pep_path
        self.rec_path = rec_path
        if cache_dir is None:
            self.cache_dir = Path.cwd() / ".cache"
        else:
            self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.script_path = script_path
        self.results_dir = results_dir
        self.kwargs = kwargs

    def __call__(self):
        raise NotImplementedError("This method should be implemented by the subclass")

    def __str__(self):
        return f"{self.__class__.__name__}()"

    def __repr__(self):
        return self.__str__()
