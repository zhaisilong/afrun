from Bio.PDB import PDBList
from pathlib import Path
from loguru import logger

PDB_LIST = PDBList(server="https://files.wwpdb.org")


def download_pdb(pdb_id: str, data_dir: Path, force_write: bool = False) -> Path | None:
    try:
        logger.info(f"Downloading PDB ID {pdb_id} to {data_dir}")
        PDB_LIST.retrieve_pdb_file(
            pdb_id, pdir=str(data_dir), file_format="mmCif", overwrite=force_write
        )
        logger.info(f"Successfully downloaded {pdb_id}")
        return data_dir / f"{pdb_id}.cif"
    except Exception as e:
        raise ValueError(f"Error downloading PDB ID {pdb_id}: {e}")
