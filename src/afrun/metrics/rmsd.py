from pymol import cmd
from loguru import logger


class RMSD:
    def __init__(self):
        self.selection_ca = "name CA"
        self.selection_backbone = "name CA+C+N+O+CB"
        self.selection_sidechain = "not name CA+C+N+O+CB"

    def compute_rmsd(self, pred_obj, ref_obj, selection, chain_sel, method="align", cycles=5):
        sel_pred = f"{pred_obj} and ({selection}) and ({chain_sel})"
        sel_ref = f"{ref_obj} and ({selection}) and ({chain_sel})"
        logger.debug(f"sel_pred: {sel_pred}, sel_ref: {sel_ref}")
        if method == "align":
            return cmd.align(sel_pred, sel_ref, cycles=cycles)[0]
        elif method == "rms_cur":
            return cmd.rms_cur(sel_pred, sel_ref)
        else:
            raise ValueError(f"Unknown RMSD method: {method}")


    def __call__(
        self,
        ref_path: str,
        pred_path: str,
        is_monomer: bool = True,
        peptide_chain: str | None = None,
        protein_chain: str | None = None,
    ):
        ref_obj = "ref"
        pred_obj = "pred"
        cmd.reinitialize()
        cmd.load(str(ref_path), ref_obj)
        cmd.load(str(pred_path), pred_obj)

        results = {"is_monomer": is_monomer}

        if is_monomer:
            if not peptide_chain:
                raise ValueError("peptide_chain must be specified for monomer.")
            chain_sel = f"chain {peptide_chain}"
            results["rmsd_ca"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_ca, chain_sel, "align", cycles=0), 3)
            results["rmsd_ca_refine"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_ca, chain_sel, "align", cycles=5), 3)
            results["rmsd_backbone"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_backbone, chain_sel, "align", cycles=0), 3)
            results["rmsd_backbone_refine"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_backbone, chain_sel, "align", cycles=5), 3)
            results["rmsd_sidechain"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_sidechain, chain_sel, "align", cycles=0), 3)
            results["rmsd_sidechain_refine"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_sidechain, chain_sel, "align", cycles=5), 3)

            results["rmsd_combined"] = round(results["rmsd_backbone"] * 0.8 + results["rmsd_sidechain"] * 0.2, 3)
            results["rmsd_combined_refine"] = round(results["rmsd_backbone_refine"] * 0.8 + results["rmsd_sidechain_refine"] * 0.2, 3)
            return results

        else:
            if not protein_chain or not peptide_chain:
                raise ValueError("Both protein_chain and peptide_chain must be specified for complex.")

            # 支持多个蛋白链对齐（B,C,D）
            prot_sel = " or ".join([f"chain {ch.strip()}" for ch in protein_chain.split(":")])
            pep_sel = f"chain {peptide_chain}"

            # Step 1: Align using protein chains
            self.compute_rmsd(pred_obj, ref_obj, self.selection_backbone, prot_sel, method="align", cycles=5)

            # Step 2: Evaluate RMSD on peptide chain only
            results["rmsd_ca_complex"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_ca, pep_sel, "rms_cur"), 3)
            results["rmsd_backbone_complex"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_backbone, pep_sel, "rms_cur"), 3)
            results["rmsd_sidechain_complex"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_sidechain, pep_sel, "rms_cur"), 3)
            results["rmsd_combined_complex"] = round(results["rmsd_backbone_complex"] * 0.8 + results["rmsd_sidechain_complex"] * 0.2, 3)
            
            # Step 3: Align using peptide chain
            results["rmsd_ca"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_ca, pep_sel, "align", cycles=0), 3)
            results["rmsd_ca_refine"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_ca, pep_sel, "align", cycles=5), 3)
            results["rmsd_backbone"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_backbone, pep_sel, "align", cycles=0), 3)
            results["rmsd_backbone_refine"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_backbone, pep_sel, "align", cycles=5), 3)
            results["rmsd_sidechain"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_sidechain, pep_sel, "align", cycles=0), 3)
            results["rmsd_sidechain_refine"] = round(self.compute_rmsd(pred_obj, ref_obj, self.selection_sidechain, pep_sel, "align", cycles=5), 3)

            results["rmsd_combined"] = round(results["rmsd_backbone"] * 0.8 + results["rmsd_sidechain"] * 0.2, 3)
            results["rmsd_combined_refine"] = round(results["rmsd_backbone_refine"] * 0.8 + results["rmsd_sidechain_refine"] * 0.2, 3)
            return results
