# AFRUN

- **Run AlphaFold3 in one click**

## Setups

```bash
# Python Env
mamba create -n astra python=3.12
mamba activate astra

pip install torch==2.7.1 torchvision==0.22.1 torchaudio==2.7.1 --index-url https://download.pytorch.org/whl/cu126
pip install -e .
```

## Datasets and Precomputed MSA Resource

- precmputed msa resource can be found at [zonodo](https://google.com)

```bash

```

## Usages

```bash

cd afrun
```

### CMD

```bash
afrun predict

afrun msa status
afrun msa list

# --db_path was specified by default
afrun msa --db_path /home/silong/projects/afcp/karma/alphafold3/msa/msadb_af3.sqlite
afrun msa add /home/silong/projects/afcp/cases/2flu/af3/2flu/2flu_data.json
afrun msa update /home/silong/projects/afcp/cases/2flu/af3/2flu/2flu_data.json

# search by hash256
afrun msa search fb473d1ba435ed43a6114104dda77679ab742c63
```

### Python Packages

```python
from afrun.msa.msadb import MSADB, MSA
from astra.data.sequence import sha256_hash

dmp_sequence_list = ["CESQVSQSVSSSPC", "CSQVSQSVSSSPFC", "CESQVSQSVSSSPFYC"]
tl1a_seq = "MAEDLGLSFGETASVEMLPEHGSCRPKARSSSARWALTCCLVLLPFLAGLTTYLLVSQLRAQGEACVQFQALKGQEFAPSHQQVYAPLRADGDKPRAHLTVVRQTPTQHFKNQFPALHWEHELGLAFTKNRMNYTNKFLLIPESGDYFIYSQVTFRGMTSECSEIRQAGRPNKPDSITVVITKVTDSYPEPTQLLMGTKSVCEVGSNWFQPIYLGAMFSLQEGDKLMVNVSDISLVDYTKEDKTFFGAFLL"
tl1a_msa_id = msadb.search(sha256_hash(tl1a_seq))[0]
tl1a_msa = msadb.get_msa(tl1a_msa_id)

# to use
tl1a_msa["id"] = "A" # please replace this

# update
msadb = MSADB("/home/silong/projects/astra/karma/alphafold3/msa/msadb_af3.sqlite")
for x in template_data["sequences"]:
    protein = x["protein"]
    msa = MSA(
        sequence=protein.get("sequence"),
        unpairedMsa=protein.get("unpairedMsa"),
        pairedMsa=protein.get("pairedMsa"),
        templates=protein.get("templates"),
    )
    msadb.upsert_msa(msa)

msadb.close()
```
