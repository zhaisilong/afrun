# ======= Standard Library =======
import collections
from io import StringIO
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import os

# ======= Third-party Libraries =======
import numpy as np
from loguru import logger
from Bio.PDB import MMCIFParser, PDBIO
from pymol import cmd
from rdkit import Chem
from rdkit.Chem import (
    AllChem,
    Descriptors,
    Draw,
    MolToSmiles,
    MolToInchi,
    MolToInchiKey,
    rdMolDescriptors,
)
from pdbfixer import PDBFixer
from pdbfixer.pdbfixer import Template
import openmm.app as app
from openmm.app import PDBFile, Topology

# ======= Project-Specific Modules =======
# from alphafold3.data.tools.rdkit_utils import (
#     assign_atom_names_from_graph,
#     _RDKIT_BOND_TYPE_TO_MMCIF,
#     _RDKIT_BOND_STEREO_TO_MMCIF,
#     UnsupportedMolBondError,
# )
# from alphafold3.cpp import cif_dict
import importlib.util
from .config_utils import rename_by_prefix
from pdbtools import (
    pdb_wc,
    pdb_delelem,
    pdb_fromcif,
)
from Bio.PDB import PDBParser, PDBIO, Select, StructureBuilder
from Bio.PDB.Polypeptide import is_aa


class _PDBFixer(PDBFixer):
    def __init__(self, file_io):
        self._initializeFromPDB(file_io)
        # Check the structure has some atoms in it.
        atoms = list(self.topology.atoms())
        if len(atoms) == 0:
            raise Exception("Structure contains no atoms.")

        # Keep a cache of downloaded CCD definitions
        self._ccdCache = {}

        # Load the templates.
        self.templates = {}
        self._standardTemplates = set()
        templatesPath = os.path.join(os.path.dirname(__file__), "templates")
        for file in os.listdir(templatesPath):
            templatePdb = app.PDBFile(os.path.join(templatesPath, file))
            name = next(templatePdb.topology.residues()).name
            self.templates[name] = Template(templatePdb.topology, templatePdb.positions)
            self._standardTemplates.add(name)


def run_pdbtools(pdb_io: StringIO, func_name: Any, *args, **kwargs) -> str:
    return "".join(func_name.run(pdb_io, *args, **kwargs))


class ChainRenamerAndCleaner(Select):
    def __init__(self, chain_map=None, atom_exclude=None):
        """
        Args:
            chain_map: dict, e.g., {"A": "X", "B": "Y"}
            atom_exclude: set of atom names to remove, e.g., {"H", "HOH"}
        """
        self.chain_map = chain_map or {}
        self.atom_exclude = atom_exclude or set()

    def accept_chain(self, chain):
        return True  # Keep all chains, we'll rename them

    def accept_residue(self, residue):
        hetfield, resseq, icode = residue.get_id()
        if hetfield.strip() == "W":  # Remove water (HOH)
            return False
        return True

    def accept_atom(self, atom):
        return atom.element not in self.atom_exclude


class PDBPreparer:
    def __init__(
        self,
        force_write: bool = False,
        radius: float = 10.0,
        verbose: bool = False,
    ):
        self.force_write = force_write
        self.radius = radius
        self.verbose = verbose

    @staticmethod
    def fix_pdb_old(
        pdb_io: StringIO, save_path: Path, ff=None, chain_map=None, remove_atoms=None
    ):
        fixer = _PDBFixer(pdb_io)

        # Find missing residues
        missing_residues = fixer.findMissingResidues()
        if missing_residues:
            logger.warning(f"Found missing residues: {missing_residues}")

        # Find nonstandard residues
        nonstandard_residues = fixer.findNonstandardResidues()
        if nonstandard_residues:
            logger.warning(f"Found nonstandard residues: {nonstandard_residues}")
            fixer.downloadTemplate(nonstandard_residues[0].name)
        #     logger.warning(f"Found nonstandard residues: {nonstandard_residues}")
        #     fixer.replaceNonstandardResidues()

        # Find missing atoms
        # missing_atoms = fixer.findMissingAtoms()
        # if missing_atoms:
        #     logger.warning(f"Found missing atoms: {missing_atoms}")
        #     fixer.addMissingAtoms()

        # remove heterogens
        # fixer.removeHeterogens(False)
        # toDelete = []
        # not_keep = ["HOH", "CL", "ZN", "MN"]
        # for residue in fixer.topology.residues():
        #     if residue.name in not_keep:
        #         toDelete.append(residue)
        # modeller = app.Modeller(fixer.topology, fixer.positions)
        # modeller.delete(toDelete)
        # fixer.topology = modeller.topology
        # fixer.positions = modeller.positions
        # logger.debug(f"toDelete: {toDelete}")

        # fixer.addMissingHydrogens()
        # if chain_map is not None:
        #     for chain in fixer.topology.chains():
        #         old_id = chain.id
        #         if old_id in chain_map:
        #             chain.id = chain_map[old_id]

        with save_path.open("w") as f:
            PDBFile.writeFile(fixer.topology, fixer.positions, f, keepIds=False)

    @staticmethod
    def fix_pdb(pdb_io: StringIO, save_path: Path, chain_map=None, atom_exclude={"Cl"}):
        parser = PDBParser(QUIET=True)
        structure = parser.get_structure("input", pdb_io)

        for model in structure:
            for chain in model:
                if chain_map is not None:
                    if chain.id in chain_map:
                        chain.id = chain_map[chain.id]
                else:
                    chain.id = chain.id.strip()

                # Renumber residues
                for i, residue in enumerate(chain.get_residues(), start=1):
                    hetfield, _, icode = residue.id
                    residue.id = (hetfield, i, icode)

        io = PDBIO()
        io.set_structure(structure)
        with save_path.open("w") as f:
            io.save(f, ChainRenamerAndCleaner(atom_exclude))

    def prepare_pdb(
        self,
        pdb_path: Path,
        is_monomer: bool = False,
        verbose: bool = False,
        keep_hydrogen: bool = False,
        chain_map: Dict[str, str] = None,
    ) -> Path:
        save_path = rename_by_prefix(pdb_path, "clean")
        if save_path.exists() and not self.force_write:
            logger.info(f"File {save_path} already exists")
            return save_path
        else:
            if verbose:
                with pdb_path.open("r") as f:
                    has_error = pdb_wc.run(f, None)
                    logger.info(f"Has error: {'Yes' if has_error else 'No'}")
            if pdb_path.suffix == ".cif":
                with pdb_path.open("r") as f:
                    pdb_str = run_pdbtools(f, pdb_fromcif)
            else:
                with pdb_path.open("r") as f:
                    if not keep_hydrogen:
                        pdb_str = run_pdbtools(f, pdb_delelem, {"H"})
                    else:
                        pdb_str = f.read()

        if is_monomer:
            return self._process_monomer(save_path, pdb_str, chain_map, verbose)
        else:
            return self._process_complex(save_path, pdb_str, chain_map, verbose)

    def _process_monomer(
        self,
        pdb_path: Path,
        pdb_str: str,
        chain_map: Dict[str, str],
        verbose: bool = False,
    ) -> Path:
        self.fix_pdb(StringIO(pdb_str), pdb_path, chain_map)
        return pdb_path

    def _process_complex(
        self,
        pdb_path: Path,
        pdb_str: str,
        chain_map: Dict[str, str],
        verbose: bool = False,
    ) -> Path:
        self.fix_pdb(StringIO(pdb_str), pdb_path, chain_map)
        return pdb_path


def split_mmcif_to_models(
    cif_file: Path,
    output_dir: Path,
    pdb_name: str = None,
    chain_map: Dict[str, str] = None,
    force_write: bool = False,
):
    parser = MMCIFParser(QUIET=True)

    pdb_name = pdb_name or cif_file.stem
    structure = parser.get_structure(pdb_name, cif_file)

    refer_paths = []
    for i, model in enumerate(structure):
        io = PDBIO()
        io.set_structure(model)
        out_path = output_dir / f"{pdb_name}_{i}" / "struct.pdb"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if not out_path.exists() or force_write:
            io.save(str(out_path))
            logger.info(f"Saved: {str(out_path)}")

        refer_paths.append(out_path)
    return refer_paths


def mol_to_ccd_cif(
    mol: Chem.Mol,
    component_id: str,
    pdbx_smiles: Optional[str] = None,
    include_hydrogens: bool = True,
) -> "cif_dict.CifDict":
    """
    将 RDKit Mol 转为 CCD‑style mmCIF 字典，满足 AlphaFold‑3 输入要求。
    """
    mol = Chem.Mol(mol)  # 复制，避免副作用
    if include_hydrogens:
        mol = Chem.AddHs(mol)
    Chem.Kekulize(mol)

    # 坐标（若存在）转为格式化字符串
    if mol.GetNumConformers():
        coords = mol.GetConformer(0).GetPositions()
        ideal_coords = np.round(coords, 3).astype(str)  # 比 np.vectorize 更快
    else:
        ideal_coords = None

    # ----------  chem_comp  ----------
    mol_cif: dict[str, list[str]] = collections.defaultdict(list)
    mol_cif["data_"] = [component_id]

    mol_cif["_chem_comp.id"] = [component_id]
    mol_cif["_chem_comp.mon_nstd_parent_comp_id"] = [component_id]  # 简单设为自身
    mol_cif["_chem_comp.name"] = [
        mol.GetProp("_Name") if mol.HasProp("_Name") else component_id
    ]
    mol_cif["_chem_comp.pdbx_synonyms"] = ["."]  # 无同义词
    mol_cif["_chem_comp.type"] = ["NON-POLYMER"]  # 常见小分子类型

    mol_cif["_chem_comp.formula"] = [rdMolDescriptors.CalcMolFormula(mol)]
    mol_cif["_chem_comp.formula_weight"] = [f"{Descriptors.ExactMolWt(mol):.3f}"]
    if pdbx_smiles:
        mol_cif["_chem_comp.pdbx_smiles"] = [pdbx_smiles]

    # ----------  pdbx_chem_comp_descriptor  ----------
    canonical_smiles = MolToSmiles(mol, isomericSmiles=True, canonical=True)
    inchi = MolToInchi(mol)
    inchikey = MolToInchiKey(mol)

    for dtype, desc in [
        ("SMILES_CANONICAL", canonical_smiles),
        ("InChI", inchi),
        ("InChIKey", inchikey),
    ]:
        mol_cif["_pdbx_chem_comp_descriptor.comp_id"].append(component_id)
        mol_cif["_pdbx_chem_comp_descriptor.type"].append(dtype)
        mol_cif["_pdbx_chem_comp_descriptor.descriptor"].append(desc)
        # 以下两列不是 AF‑3 必需，但放占位符可保持列长度一致
        mol_cif["_pdbx_chem_comp_descriptor.program"].append(".")
        mol_cif["_pdbx_chem_comp_descriptor.program_version"].append(".")

    # ----------  chem_comp_atom  ----------
    mol = assign_atom_names_from_graph(mol, keep_existing_names=True)
    for idx, atom in enumerate(mol.GetAtoms()):
        if not include_hydrogens and atom.GetSymbol() in ("H", "D"):
            continue

        mol_cif["_chem_comp_atom.comp_id"].append(component_id)
        mol_cif["_chem_comp_atom.atom_id"].append(atom.GetProp("atom_name"))
        mol_cif["_chem_comp_atom.type_symbol"].append(atom.GetSymbol().upper())
        mol_cif["_chem_comp_atom.charge"].append(str(atom.GetFormalCharge()))

        if ideal_coords is not None:
            x, y, z = ideal_coords[idx]
            mol_cif["_chem_comp_atom.pdbx_model_Cartn_x_ideal"].append(x)
            mol_cif["_chem_comp_atom.pdbx_model_Cartn_y_ideal"].append(y)
            mol_cif["_chem_comp_atom.pdbx_model_Cartn_z_ideal"].append(z)

    # ----------  chem_comp_bond  ----------
    for bond in mol.GetBonds():
        a1, a2 = bond.GetBeginAtom(), bond.GetEndAtom()
        if not include_hydrogens and (
            a1.GetSymbol() in ("H", "D") or a2.GetSymbol() in ("H", "D")
        ):
            continue

        mol_cif["_chem_comp_bond.comp_id"].append(component_id)
        mol_cif["_chem_comp_bond.atom_id_1"].append(a1.GetProp("atom_name"))
        mol_cif["_chem_comp_bond.atom_id_2"].append(a2.GetProp("atom_name"))

        try:
            btype = bond.GetBondType()
            if btype == Chem.rdchem.BondType.DATIVE:
                btype = Chem.rdchem.BondType.SINGLE
            mol_cif["_chem_comp_bond.value_order"].append(
                _RDKIT_BOND_TYPE_TO_MMCIF[btype]
            )
            mol_cif["_chem_comp_bond.pdbx_stereo_config"].append(
                _RDKIT_BOND_STEREO_TO_MMCIF[bond.GetStereo()]
            )
        except KeyError as exc:
            raise UnsupportedMolBondError(str(exc)) from exc

        mol_cif["_chem_comp_bond.pdbx_aromatic_flag"].append(
            "Y" if bond.GetIsAromatic() else "N"
        )

    return cif_dict.CifDict(mol_cif)


def smiles_to_mol(
    smiles: str, show_xyz: bool = False, return_img: bool = False
) -> Chem.Mol:
    mol = Chem.MolFromSmiles(smiles)
    AllChem.EmbedMolecule(mol, AllChem.ETKDG())
    AllChem.UFFOptimizeMolecule(mol)
    if show_xyz:
        conf = mol.GetConformer()
        for atom in mol.GetAtoms():
            pos = conf.GetAtomPosition(atom.GetIdx())
            print(
                f"Atom {atom.GetSymbol()} - x: {pos.x:.3f}, y: {pos.y:.3f}, z: {pos.z:.3f}"
            )
    if return_img:
        opts = Draw.MolDrawOptions()
        opts.addAtomIndices = True
        img = Draw.MolToImage(mol, size=(300, 300), options=opts)
        return mol, img
    else:
        return mol


def cif2pdb(cif_path: Path, force_write: bool = False):
    assert cif_path.suffix == ".cif", "the cif file should have a .cif suffix"
    try:
        pdb_path = cif_path.with_suffix(".pdb")
        if pdb_path.exists() and not force_write:
            logger.info(f"{pdb_path} exists skip")
            return pdb_path
        maxit_cmd = [
            "maxit",
            "-input",
            str(cif_path),
            "-output",
            str(pdb_path),
            "-o",
            "2",
        ]
        logger.debug(f"Maxit CIF to PDB: {' '.join(maxit_cmd)}")
        result = subprocess.run(maxit_cmd, check=True)
        return pdb_path
    except Exception as e:
        raise RuntimeError(f"Failed to convert CIF to PDB: {cif_path} since {e}")


def pdb2cif(pdb_path: Path, force_write: bool = False) -> Path:
    assert pdb_path.suffix == ".pdb", "the pdb file should have a .pdb suffix"
    try:
        cif_path = pdb_path.with_suffix(".cif")
        if cif_path.exists() and not force_write:
            logger.info(f"{cif_path} exists skip")
            return cif_path
        maxit_cmd = [
            "maxit",
            "-input",
            str(pdb_path),
            "-output",
            str(cif_path),
            "-o",
            "1",
        ]
        logger.debug(f"Maxit PDB to CIF: {' '.join(maxit_cmd)}")
        result = subprocess.run(maxit_cmd, capture_output=True, text=True, check=True)
        return cif_path
    except Exception as e:
        raise RuntimeError(f"Failed to convert PDB to CIF: {pdb_path} since {e}")


def cif2cif(cif_path: Path, force_write: bool = False) -> Path:
    assert cif_path.suffix == ".cif", "the cif file should have a .cif suffix"
    try:
        new_cif_path = cif_path.with_suffix(".maxit.cif")
        if new_cif_path.exists() and not force_write:
            logger.info(f"{new_cif_path} exists skip")
            return new_cif_path
        maxit_cmd = [
            "maxit",
            "-input",
            str(cif_path),
            "-output",
            str(new_cif_path),
            "-o",
            "8",
        ]
        logger.debug(f"Maxit CIF to CIF: {' '.join(maxit_cmd)}")
        result = subprocess.run(maxit_cmd, capture_output=True, text=True, check=True)
        return new_cif_path
    except Exception as e:
        raise RuntimeError(f"Failed to convert CIF to CIF: {cif_path} since {e}")


def cal_rmsd(pred_pdb: Path, native_pdb: Path, selection="name N+CA+C"):
    """
    使用 PyMOL 计算 RMSD，对象名避免使用保留字
    """
    cmd.reinitialize()
    cmd.load(str(pred_pdb), "pred")
    cmd.load(str(native_pdb), "native")  # 替换掉 'model'

    # 注意这里也要用 'ref'
    rmsd = cmd.align(f"bound and {selection}", f"ref and {selection}")[0]
    return rmsd
