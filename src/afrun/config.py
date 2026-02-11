from typing import Optional
from loguru import logger

from . import utils


def gen_af3_config(
    job_info: dict,
    job_name: str,
    modelSeeds: list[int] = [42, 55, 1],
    chain_filters: Optional[list[str]] = None,
) -> dict:
    sequences = []

    for chain_info in job_info["polymer"]:
        if not isinstance(chain_info, dict):
            continue
        if (chain_info["id"] not in chain_filters) and (chain_filters is not None):
            logger.warning(f"Chain {chain_info['id']} is not in chain_filters")
            continue

        sequence = chain_info["sequence"]
        if chain_info["has_nonstd"]:
            modifications = []
            for pos, ncaa_three in zip(chain_info["positions"], chain_info["types"]):
                pos = int(pos)
                sequence = utils.replace_char(
                    sequence,
                    pos - 1,
                    utils.letters_three_to_one(ncaa_three, default="X"),
                )
                modifications.append(
                    {"ptmType": ncaa_three, "ptmPosition": pos},
                )
        else:
            modifications = []
            sequence = chain_info["sequence"]

        sequences.append(
            {
                "protein": {
                    "id": chain_info["id"],
                    "sequence": sequence,
                    "modifications": modifications,
                    # "unpairedMsa": "",
                    # "pairedMsa": "",
                    # "templates": [],
                }
            }
        )
    af3_dict = {
        "name": job_name,
        "modelSeeds": modelSeeds,
        "sequences": sequences,
        "dialect": "alphafold3",
        "version": 1,
    }
    return af3_dict


def cons2pairs(con: list[dict]) -> list[list[str, str]]:
    output = []
    for connected_pair in con:
        ptnr1 = connected_pair["ptnr1"]
        ptnr2 = connected_pair["ptnr2"]

        output.append(
            [
                [ptnr1["chain"], int(ptnr1["seq_id"]), ptnr1["atom"]],
                [ptnr2["chain"], int(ptnr2["seq_id"]), ptnr2["atom"]],
            ]
        )
    return output


def gen_af3_config_cyclic(
    job_info: dict,
    job_name: str,
    modelSeeds: list[int] = [42, 55, 1],
    chain_filters: Optional[list[str]] = None,
    link_chains: Optional[list[str]] = None,
) -> dict:
    """
    link_chains: list of chains that are considered as self-connected
    """
    sequences = []

    for chain_info in job_info["polymer"]:
        if not isinstance(chain_info, dict):
            continue
        if (chain_info["id"] not in chain_filters) and (chain_filters is not None):
            logger.warning(f"Chain {chain_info['id']} is not in chain_filters")
            continue

        sequence = chain_info["sequence"]
        if chain_info["has_nonstd"]:
            modifications = []
            for pos, ncaa_three in zip(chain_info["positions"], chain_info["types"]):
                pos = int(pos)
                sequence = utils.replace_char(
                    sequence,
                    pos - 1,
                    utils.letters_three_to_one(ncaa_three, default="X"),
                )
                modifications.append(
                    {"ptmType": ncaa_three, "ptmPosition": pos},
                )
        else:
            modifications = []
            sequence = chain_info["sequence"]

        sequences.append(
            {
                "protein": {
                    "id": chain_info["id"],
                    "sequence": sequence,
                    "modifications": modifications,
                    # "unpairedMsa": "",
                    # "pairedMsa": "",
                    # "templates": [],
                }
            }
        )

    conns = []
    if job_info.get("connect", []):
        for connected_pair in job_info["connect"]:
            ptnr1 = connected_pair["ptnr1"]
            ptnr2 = connected_pair["ptnr2"]
            if (ptnr1["chain"] in link_chains) and (ptnr2["chain"] in link_chains):
                conns.append(connected_pair)

    af3_dict = {
        "name": job_name,
        "modelSeeds": modelSeeds,
        "sequences": sequences,
        "bondedAtomPairs": cons2pairs(conns),
        "dialect": "alphafold3",
        "version": 1,
    }
    return af3_dict
