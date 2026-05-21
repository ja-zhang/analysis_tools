import math
import awkward as ak
import numpy as np
import matplotlib.pyplot as plt

from typing import Callable, Any, Optional, Union, Tuple

from dataclasses import dataclass

try:
    from IPython.display import clear_output as _ipython_clear_output
    _IPYTHON_AVAILABLE = True
except ImportError:
    _IPYTHON_AVAILABLE = False


@dataclass
class Cut:
    """
    Generic cut object.
    """

    name:      str
    variable:  str
    func:      Callable[[Any], Any]
    threshold: Optional[Union[float, Tuple[float, float]]] = None
    direction: Optional[str] = None   # "above", "below", "between", or None for pass-all

    def apply(self, batch):
        """
        Returns boolean mask.
        """
        values = batch[self.variable]
        return self.func(values)


    @staticmethod
    def greater_than(threshold):
        return lambda x: x > threshold

    @staticmethod
    def less_than(threshold):
        return lambda x: x < threshold

    @staticmethod
    def between(low, high):
        return lambda x: (x > low) & (x < high)

    @staticmethod
    def true():
        return lambda x: x == x

    @staticmethod
    def false():
        return lambda x: x != x

    def _give_name(self):
        return self.name

    def _give_variable(self):
        return self.variable

    def _give_function(self):
        return self.func



class BeamSelection:

    def __init__(self, name):
        self.name   = name
        self.cuts   = []
        self._write_parquet  = False
        self._parquet_path   = None
        self._parquet_writer = None

    def add(self, cut: Cut):
        self.cuts.append(cut)

    def mask(self, batch):
        """
        Combined AND of all cuts.
        """
        mask = self.cuts[0].apply(batch)
        for cut in self.cuts[1:]:
            mask = cut.apply(batch) & mask
        return mask

    def describe(self):
        """Print the cuts applied by this selection with threshold values and directions."""
        print(f"Selection : {self.name}")
        for cut in self.cuts:
            unit = _VARIABLE_UNITS.get(cut.variable, "")
            unit_str = f" {unit}" if unit else ""
            if cut.threshold is None:
                value_str = "pass all (no TOF separation available for this run)"
            elif cut.direction == "above":
                value_str = f"> {cut.threshold:.3g}{unit_str}  [above threshold]"
            elif cut.direction == "below":
                value_str = f"< {cut.threshold:.3g}{unit_str}  [below threshold]"
            elif cut.direction == "between":
                lo, hi = cut.threshold
                value_str = f"between {lo:.3g} and {hi:.3g}{unit_str}"
            else:
                value_str = str(cut.threshold)
            print(f"  {cut.variable:<22} {value_str}")

    def enable_parquet_output(self, path=None):
        """
        Enable writing selected events to a parquet file during iteration.
        Call this before the batch loop, then call close_parquet_writer() after.

        Parameters
        ----------
        path : str, optional
            Output file path. Defaults to "<selection_name>.parquet".
        """
        self._parquet_path   = path if path is not None else f"{self.name}.parquet"
        self._write_parquet  = True
        self._parquet_writer = None

    def _write_to_parquet(self, batch):
        if not self._write_parquet or len(batch) == 0:
            return
        import pyarrow.parquet as pq
        table = ak.to_arrow_table(batch)
        if self._parquet_writer is None:
            self._parquet_writer = pq.ParquetWriter(self._parquet_path, table.schema)
        self._parquet_writer.write_table(table)

    def close_parquet_writer(self):
        if self._parquet_writer is not None:
            self._parquet_writer.close()
            self._parquet_writer = None
            print(f"Wrote {self.name} events to: {self._parquet_path}")

    # ------------------------------------------------------------------
    # Named constructors — one per particle type.
    # Thresholds are explicit so users can see and modify them directly.
    # ------------------------------------------------------------------

    @classmethod
    def electron(cls, act_eveto_cut, proton_tof_cut=0):
        """
        Electrons: fast particles (below proton TOF) that are above threshold
        in the upstream ACT (act_eveto), which only electrons trigger at most momenta.

        Parameters
        ----------
        act_eveto_cut  : float  charge threshold in the upstream ACT [pC]
        proton_tof_cut : float  TOF value separating fast from slow particles [ns]
                                set to 0 if TOF separation is not available
        """
        sel = cls("electron")
        _add_fast_particle_cut(sel, proton_tof_cut)
        sel.add(Cut(
            name      = "act_eveto_above_threshold",
            variable  = "vme_act_eveto",
            func      = Cut.greater_than(act_eveto_cut),
            threshold = act_eveto_cut,
            direction = "above",
        ))
        return sel

    @classmethod
    def muon(cls, act_eveto_cut, act_tagger_cut, proton_tof_cut=0):
        """
        Muons: fast particles, below threshold in the upstream ACT (act_eveto),
        and above threshold in the downstream ACT (act_tagger).

        Parameters
        ----------
        act_eveto_cut  : float  upstream ACT charge threshold [pC]
        act_tagger_cut : float  downstream ACT charge threshold [pC]
        proton_tof_cut : float  TOF value separating fast from slow particles [ns]
                                set to 0 if TOF separation is not available
        """
        sel = cls("muon")
        _add_fast_particle_cut(sel, proton_tof_cut)
        sel.add(Cut(
            name      = "act_eveto_below_threshold",
            variable  = "vme_act_eveto",
            func      = Cut.less_than(act_eveto_cut),
            threshold = act_eveto_cut,
            direction = "below",
        ))
        sel.add(Cut(
            name      = "act_tagger_above_threshold",
            variable  = "vme_act_tagger",
            func      = Cut.greater_than(act_tagger_cut),
            threshold = act_tagger_cut,
            direction = "above",
        ))
        return sel

    @classmethod
    def kaon(cls, act_eveto_cut, act_tagger_cut, proton_tof_cut=0):
        """
        Kaons: fast particles, below threshold in the upstream ACT (act_eveto),
        and above threshold in the downstream ACT (act_tagger).

        Parameters
        ----------
        act_eveto_cut  : float  upstream ACT charge threshold [pC]
        act_tagger_cut : float  downstream ACT charge threshold [pC]
        proton_tof_cut : float  TOF value separating fast from slow particles [ns]
                                set to 0 if TOF separation is not available
        """
        sel = cls("kaon")
        _add_fast_particle_cut(sel, proton_tof_cut)
        sel.add(Cut(
            name      = "act_eveto_below_threshold",
            variable  = "vme_act_eveto",
            func      = Cut.less_than(act_eveto_cut),
            threshold = act_eveto_cut,
            direction = "below",
        ))
        sel.add(Cut(
            name      = "act_tagger_above_threshold",
            variable  = "vme_act_tagger",
            func      = Cut.greater_than(act_tagger_cut),
            threshold = act_tagger_cut,
            direction = "above",
        ))
        return sel

    @classmethod
    def pion(cls, act_eveto_cut, act_tagger_cut, proton_tof_cut=0):
        """
        Pions: fast particles, below threshold in both ACTs.

        Parameters
        ----------
        act_eveto_cut  : float  upstream ACT charge threshold [pC]
        act_tagger_cut : float  downstream ACT charge threshold [pC]
        proton_tof_cut : float  TOF value separating fast from slow particles [ns]
                                set to 0 if TOF separation is not available
        """
        sel = cls("pion")
        _add_fast_particle_cut(sel, proton_tof_cut)
        sel.add(Cut(
            name      = "act_eveto_below_threshold",
            variable  = "vme_act_eveto",
            func      = Cut.less_than(act_eveto_cut),
            threshold = act_eveto_cut,
            direction = "below",
        ))
        sel.add(Cut(
            name      = "act_tagger_below_threshold",
            variable  = "vme_act_tagger",
            func      = Cut.less_than(act_tagger_cut),
            threshold = act_tagger_cut,
            direction = "below",
        ))
        return sel

    @classmethod
    def proton(cls, proton_tof_cut, proton_tof_window=30):
        """
        Protons: TOF falls within a window above the fast/slow separation value.
        Only usable when proton TOF information is available (proton_tof_cut > 0).

        Parameters
        ----------
        proton_tof_cut    : float  lower edge of the proton TOF window [ns]
        proton_tof_window : float  width of the TOF window [ns], default 30
        """
        if proton_tof_cut <= 1e-3 or math.isnan(proton_tof_cut):
            raise ValueError(
                "proton_tof_cut must be > 0 to build a proton selection. "
                "This run does not have TOF separation available."
            )
        sel = cls("proton")
        sel.add(Cut(
            name      = "proton_tof_window",
            variable  = "vme_tof_corr",
            func      = Cut.between(proton_tof_cut, proton_tof_cut + proton_tof_window),
            threshold = (proton_tof_cut, proton_tof_cut + proton_tof_window),
            direction = "between",
        ))
        return sel


def _add_fast_particle_cut(sel, proton_tof_cut):
    """Add a fast-particle TOF cut to sel, or a pass-all cut if TOF is unavailable."""
    tof = float(proton_tof_cut)
    if tof <= 1e-3 or math.isnan(tof):
        sel.add(Cut(
            name      = "fast_particle_no_tof_info",
            variable  = "vme_tof_corr",
            func      = Cut.true(),
            threshold = None,
        ))
    else:
        sel.add(Cut(
            name      = "fast_particle",
            variable  = "vme_tof_corr",
            func      = Cut.less_than(tof),
            threshold = tof,
            direction = "below",
        ))


_VARIABLE_UNITS = {
    "vme_act_eveto":  "PE",
    "vme_act_tagger": "PE",
    "vme_tof_corr":   "ns",
}


class SelectionMonitor:
    """
    Accumulates per-variable histograms across batches and shows live-updating
    plots with cut lines taken from the BeamSelection objects.

    Each plot shows all events in gray with each particle selection overlaid
    in a distinct colour. Cut lines are labelled to show whether the selection
    requires events to be above or below that threshold.

    Parameters
    ----------
    selections   : list of BeamSelection
    update_every : int       — redraw the plot every N calls to update() (default 10)
    bins         : int       — number of histogram bins (default 100)
    vme_run_info : optional  — awkward record from loader.get_vme_analysis_run_info();
                               if provided, adds run number, momentum and ACT cut values
                               to the figure title
    """

    def __init__(self, selections, update_every=10, bins=100, vme_run_info=None):
        self._selections   = selections
        self._update_every = update_every
        self._bins         = bins
        self._call_count   = 0
        self._run_info     = vme_run_info

        seen, self._variables = set(), []
        for sel in selections:
            for cut in sel.cuts:
                if cut.variable not in seen:
                    seen.add(cut.variable)
                    self._variables.append(cut.variable)

        self._counts     = {v: None for v in self._variables}
        self._edges      = {v: None for v in self._variables}
        self._sel_counts = {sel.name: {v: None for v in self._variables}
                            for sel in selections}

    def _get_cut_threshold(self, variable):
        """Return the first non-None threshold found for this variable across all selections."""
        for sel in self._selections:
            for cut in sel.cuts:
                if cut.variable == variable and cut.threshold is not None:
                    return cut.threshold
        return None

    def update(self, batch):
        masked = {sel.name: batch[sel.mask(batch)] for sel in self._selections}
        self._accumulate(batch, masked)
        self._call_count += 1
        if self._call_count % self._update_every == 0:
            self.show()

    def _accumulate(self, batch, masked_batches):
        for var in self._variables:
            if var not in ak.fields(batch):
                continue
            values = ak.to_numpy(batch[var])
            values = values[np.isfinite(values)]
            if len(values) == 0:
                continue
            if self._edges[var] is None:
                lo, hi = float(values.min() * 0.8), float(values.max() * 1.2)
                if lo == hi:
                    lo, hi = lo - 1.0, hi + 1.0
                self._edges[var]  = np.linspace(lo, hi, self._bins + 1)
                self._counts[var] = np.zeros(self._bins, dtype=np.float64)
                for sel_name in self._sel_counts:
                    self._sel_counts[sel_name][var] = np.zeros(self._bins, dtype=np.float64)
            counts, _ = np.histogram(values, bins=self._edges[var])
            self._counts[var] += counts
            for sel_name, sel_batch in masked_batches.items():
                if var not in ak.fields(sel_batch):
                    continue
                sel_vals = ak.to_numpy(sel_batch[var])
                sel_vals = sel_vals[np.isfinite(sel_vals)]
                sel_counts, _ = np.histogram(sel_vals, bins=self._edges[var])
                self._sel_counts[sel_name][var] += sel_counts

    def _build_suptitle(self):
        parts = []
        if self._run_info is not None:
            parts.append(f"Run {int(self._run_info['run_number'])}")
            parts.append(f"p = {int(self._run_info['run_momentum'])} MeV/c")
            parts.append(f"n(act_eveto) = {float(self._run_info['n_eveto']):.3f}")
            parts.append(f"n(act_tagger) = {float(self._run_info['n_tagger']):.3f}")
        return "   |   ".join(parts)

    def show(self):
        n = len(self._variables)
        if n == 0:
            return
        fig, axes = plt.subplots(1, n, figsize=(5 * n, 4))
        if n == 1:
            axes = [axes]
        colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

        for ax, var in zip(axes, self._variables):
            counts, edges = self._counts[var], self._edges[var]
            if counts is None:
                ax.set_title(var)
                ax.text(0.5, 0.5, "no data yet", transform=ax.transAxes,
                        ha="center", va="center")
                continue

            unit    = _VARIABLE_UNITS.get(var, "")
            unit_str = f" {unit}" if unit else ""
            centers = 0.5 * (edges[:-1] + edges[1:])
            safe    = np.maximum(counts, 0.5)

            # Gray background: all events
            ax.fill_between(centers, safe, step="mid",
                            color="gray", alpha=0.25, label="all events")
            ax.step(centers, safe, where="mid", color="gray", alpha=0.6, linewidth=1)

            # Coloured overlays: per-selection events
            for i, sel in enumerate(self._selections):
                color      = colors[i % len(colors)]
                sel_counts = self._sel_counts[sel.name][var]
                if sel_counts is not None:
                    ax.step(centers, np.maximum(sel_counts, 0.5), where="mid",
                            color=color, linewidth=1.8, label=sel.name)

            # Cut lines with informative labels
            for i, sel in enumerate(self._selections):
                color = colors[i % len(colors)]
                for cut in sel.cuts:
                    if cut.variable != var or cut.threshold is None:
                        continue
                    if cut.direction == "between":
                        lo, hi = cut.threshold
                        ax.axvline(lo, color=color, linestyle="--", linewidth=1.2,
                                   label=f"{sel.name} between {lo:.2g}–{hi:.2g}{unit_str}")
                        ax.axvline(hi, color=color, linestyle="--", linewidth=1.2)
                    elif cut.direction == "above":
                        ax.axvline(cut.threshold, color=color, linestyle="--", linewidth=1.2,
                                   label=f"{sel.name} above {cut.threshold:.2g}{unit_str}")
                    elif cut.direction == "below":
                        ax.axvline(cut.threshold, color=color, linestyle="--", linewidth=1.2,
                                   label=f"{sel.name} below {cut.threshold:.2g}{unit_str}")

            ax.set_yscale("log")
            ax.set_xlabel(f"{var} [{unit}]" if unit else var)
            ax.set_ylabel("Counts")
            ax.set_title(var)
            ax.legend(fontsize=7, loc="best")

        suptitle = self._build_suptitle()
        if suptitle:
            fig.suptitle(suptitle, fontsize=14, weight="bold")
            fig.tight_layout(rect=[0, 0, 1, 0.93])
        else:
            fig.tight_layout()
        if _IPYTHON_AVAILABLE:
            _ipython_clear_output(wait=True)
        plt.show()
        plt.close(fig)


# Particle masses in MeV/c²
_PARTICLE_MASSES = {
    "electron": 0.511,
    "muon":     105.66,
    "pion":     139.57,
    "kaon":     493.68,
    "proton":   938.27,
    "deuteron": 1876.54,
    "helium3":  2808.39,
}


def print_cherenkov_thresholds(vme_run_info):
    """
    Print which particles are above the Cherenkov threshold in each ACT for this run.

    A particle produces light in an ACT only if its momentum exceeds the threshold
    p_threshold = mass / sqrt(n^2 - 1), where n is the refractive index of the radiator.
    This determines whether the ACT-based cuts in your selection are meaningful.

    Parameters
    ----------
    vme_run_info : awkward record from loader.get_vme_analysis_run_info()
                   must contain: run_momentum [MeV/c], n_eveto, n_tagger
    """
    momentum = abs(float(vme_run_info["run_momentum"]))
    n_eveto  = float(vme_run_info["n_eveto"])
    n_tagger = float(vme_run_info["n_tagger"])

    def cherenkov_threshold(mass, n):
        return mass / math.sqrt(n ** 2 - 1)

    print(f"Run momentum : {momentum:.0f} MeV/c")
    print(f"n (act_eveto)  = {n_eveto:.4f}   n (act_tagger) = {n_tagger:.4f}")
    print()
    header = f"{'Particle':<10} {'Mass [MeV]':>10}   {'Thresh. ACT eveto [MeV]':>22}  {'Above?':>6}   {'Thresh. ACT tagger [MeV]':>23}  {'Above?':>6}"
    print(header)
    print("-" * len(header))
    for name, mass in _PARTICLE_MASSES.items():
        thr_eveto  = cherenkov_threshold(mass, n_eveto)
        thr_tagger = cherenkov_threshold(mass, n_tagger)
        above_eveto  = "yes" if momentum > thr_eveto  else "no"
        above_tagger = "yes" if momentum > thr_tagger else "no"
        print(f"{name:<10} {mass:>10.3f}   {thr_eveto:>22.1f}  {above_eveto:>6}   {thr_tagger:>23.1f}  {above_tagger:>6}")






























# ############################
# class ParticleID(IntEnum):
    
#     # not ided     
#     UNKOWN         = 0
    
#     # leptons
#     ELECTRON       = 11

#     MUON           = 13

#     # mesons
#     PION           = 211

#     # baryons
#     PROTON         = 2212

#     # nuclei
#     DEUTERON       = 1000010020

#     HELIUM3        = 1000020030
    
#     KAON
    
    
# class ParticleMass(Enum):
    
#     # not ided     
#     UNKOWN         = np.nan
    
#     # leptons
#     ELECTRON       = 0.511

#     MUON           = 105.66

#     # mesons
#     PION           = 139.57

#     # baryons
#     PROTON         = 2212

#     # nuclei
#     DEUTERON       = 1000010020

#     HELIUM3        = 1000020030
    



    

# class BeamSelection:
#     """
#     A class for applying the nominal PID cuts
#     """

#     def __init__(self, file_name):
#         #if branches to load is none then load all branches are loaded
#         self.file_name = file_name
        
#         try:
#             self.file = uproot.open(self.file_name)
#         except Exception as e:
#             raise RuntimeError(f"Failed to open ROOT file: {self.file_name}\n{e}")
            
            
#         try:
#             tree = self.file['vme_analysis_scalar_results']
#             vme_analysis_scalar_results = tree.arrays(library="ak", entry_start=0, entry_stop=1)
#             self.beam_scalar_results = vme_analysis_scalar_results[0]
#         except Exception as e:
#             raise RuntimeError(f"No VME scalar results {self.file_name}\n{e}")
            
            
#         try:
#             tree = self.file['vme_analysis_run_info']
#             vme_analysis_run_info = tree.arrays(library="ak", entry_start=0, entry_stop=1)
#             self.vme_analysis_run_info = vme_analysis_run_info[0]
            
            
#         except Exception as e:
#             raise RuntimeError(f"No VME scalar results {self.file_name}\n{e}")
            
#         self._initialised_PID_selection = False
        
        
#         #Verify that the correct particles are above threshold in ACTs, based on the nominal momentum 
#         self.check_Cherenkov_thresholds()
            
#         print("In run {run_number} with nominal momentum {"run_momentum"})
         
            
        

#     def passes_cherenkov(self, n, mass):
#         """
#         Returns True if particle is above Cherenkov threshold.
#         """
#         p_th = m / np.sqrt(n**2 - 1.0)
#         return abs(self.self.vme_analysis_run_info["run_momentum"]) > p_th
             
              
              
#    def check_Cherenkov_thresholds(self):
#        #For this momentum and refractive index, need to make sure that the particles are on the correct side of the threshold
#         #act_eveto:
#         e_above_thresh_in_act_eveto = self.passes_cherenkov(self.vme_analysis_run_info["n_eveto"], ParticleMass.ELECTRON)
#         mu_above_thresh_in_act_eveto = self.passes_cherenkov(self.vme_analysis_run_info["n_eveto"], ParticleMass.MUON)
#         pi_above_thresh_in_act_eveto = self.passes_cherenkov(self.vme_analysis_run_info["n_eveto"], ParticleMass.PION)
#         kaon_above_thresh_in_act_eveto = self.passes_cherenkov(self.vme_analysis_run_info["n_eveto"], ParticleMass.KAON)
#         #act_tagger
#         mu_above_thresh_in_act_tagger = self.passes_cherenkov(self.vme_analysis_run_info["n_tagger"], ParticleMass.MUON)
#         pi_above_thresh_in_act_tagger = self.passes_cherenkov(self.vme_analysis_run_info["n_tagger"], ParticleMass.PION)
#         kaon_above_thresh_in_act_tagger = self.passes_cherenkov(self.vme_analysis_run_info["n_tagger"], ParticleMass.KAON)
        
#         #Only apply the act_eveto selections if they are meaningful
#         self.appply_act_eveto_selection = False
        
#         #alternatively make a selection of "fast particles"
#         self.appply_act_eveto_fast_particle_tagging = False
        
#         if e_above_thresh_in_act_eveto and not mu_above_thresh_in_act_eveto and not pi_above_thresh_in_act_eveto:
#             self.appply_act_eveto_selection = True
            
#         elif e_above_thresh_in_act_eveto and mu_above_thresh_in_act_eveto and pi_above_thresh_in_act_eveto:
#             self.appply_act_eveto_fast_particle_tagging = True
    
    
        
#     def _initialise_PID_selection(self, batch):
#         """ Initialise the PID selection by creating a column in the batch dataframe with the beam PID information """
        
#         if self._initialised_PID_selection:
#             return

#         n_events = len(batch)

#         batch["beam_PID"] = np.full(
#             n_events,
#             ParticleID.UNKNOWN,
#             dtype=np.int64
#         )

#         self._initialised_PID_selection = True
        
        
#     def _set_PID(
#         self,
#         batch,
#         mask,
#         particle_id
#     ):
#         """
#         Assign PID to events passing mask.
#         """

#         already_IDed = (
#                 batch["beam_PID"] != ParticleID.UNKNOWN
#             )
#         will_be_overwritten = mask & already_IDed
        
#         if np.any(will_be_overwritten):

#             indices = np.where(will_be_overwritten)[0]

#             existing_pids = batch["beam_PID"][will_be_overwritten]

#             raise RuntimeError(
#                 f"Non-exclusive PID selection detected. "
#                 f"Attempting to assign {ParticleID(particle_id).name} "
#                 f"to {len(indices)} events already labeled as "
#                 f"{[ParticleID(pid).name for pid in existing_pids[:5]]}..."
#             )

#             batch["beam_PID"][mask] = particle_id

            

#     def apply_all_beam_PID_selections(
#             self,
#             batch,
#             verbose=False
#         ):
#             """
#             Apply all PID selections.
#             """

#             self._initialise_PID_selection(batch)

#             self._apply_beam_electron_PID(batch)
#             self._apply_beam_muon_PID(batch)
#             self._apply_beam_pion_PID(batch)
#             self._apply_beam_proton_PID(batch)
#             self._apply_beam_deuterium_PID(batch)
#             self._apply_beam_helium3_PID(batch)

#             if verbose:
#                 self.print_beam_PID_particle_content(batch)
                
                
#     def _apply_beam_electron_PID(self, batch):

#         mask = (
#             batch["act_eveto"] > self.beam_scalar_results["act_eveto_cut"]
#             & batch["tof"] < self.beam_scalar_results["proton_tof_cut"]
#         )

#         self._set_PID(
#             batch,
#             mask,
#             ParticleID.ELECTRON
#         )
        
#     def _apply_beam_muon_PID(self, batch):

#         mask = (
#             batch["act_eveto"] < self.beam_scalar_results["act_eveto_cut"]
#             & batch["tof"] < self.beam_scalar_results["proton_tof_cut"]
#             & batch["act_tagger"] > self.beam_scalar_results["act_tagger_cut"]
#         )

#         self._set_PID(
#             batch,
#             mask,
#             ParticleID.MUON
#         )
        
#     def _apply_beam_pion_PID(self, batch):

#         mask = (
#             batch["act_eveto"] < self.beam_scalar_results["act_eveto_cut"]
#             & batch["tof"] < self.beam_scalar_results["proton_tof_cut"]
#             & batch["act_tagger"] < self.beam_scalar_results["act_tagger_cut"]
#         )

#         self._set_PID(
#             batch,
#             mask,
#             ParticleID.PION 
#         )


        
#     def get_beam_PID_particle_content():
        

    
    
    
    
    
    
#     #################################################################

#     def iterate(self, verbose=False,**kwargs):
#         """Iterate over the tree in batches using uproot.iterate"""
#         defaults = {
#         "step_size": "100 MB",
#         "library": "ak",
#         }
#         defaults.update(kwargs)
#         yield from (
#             self._apply_all_data_quality_cuts(batch,verbose)
#             for batch in self.file["WCTEReadoutWindows"].iterate(
#                 expressions=self.branches_to_load,
#                 **defaults,
#             )
#         )
    
#     def _apply_all_data_quality_cuts(self, batch, verbose=False):
#         if verbose:
#             print(f"\nBatch loaded with {len(batch)} events")
#         if self.mPMT_data_quality_cuts:            
#             batch = batch[batch["window_data_quality_mask"]==0]
#             if verbose:
#                 print(f"After window_data_quality_mask cut: {len(batch)} events")

#             batch["hit_pmt_calibrated_times"] =  batch["hit_pmt_calibrated_times"][batch["hit_pmt_readout_mask"]==0]
#             batch["hit_pmt_charges"] =  batch["hit_pmt_charges"][batch["hit_pmt_readout_mask"]==0]
#             batch["hit_mpmt_slot_ids"] =  batch["hit_mpmt_slot_ids"][batch["hit_pmt_readout_mask"]==0]
#             batch["hit_pmt_position_ids"] =  batch["hit_pmt_position_ids"][batch["hit_pmt_readout_mask"]==0]
#             batch["hit_pmt_readout_mask"] =  batch["hit_pmt_readout_mask"][batch["hit_pmt_readout_mask"]==0]

#         if self.vme_event_quality_cuts:
                          
#             batch = batch[(batch["vme_digi_issues_bitmask"]==0) & (batch["vme_evt_quality_bitmask"]==0)]
#             if verbose:
#                 print(f"After vme_event_quality_cuts cut: {len(batch)} events")  
    
#         if self.t5_event_quality_cuts:
#             #a valid hit, only one hit in the main beam bunch, within time window
#             batch = batch[(batch["T5_HasValidHit"]==True)&(batch["T5_HasMultipleScintillatorsHit"]==False)&(batch["T5_HasInTimeWindow"]==True)]
#             if verbose:
#                 print(f"After t5_event_quality_cuts cut: {len(batch)} events")  

#         return batch

#     def apply_mPMT_data_quality_cuts(self):
#         self.mPMT_data_quality_cuts = True
    
#     def apply_vme_event_quality_cuts(self):
#         self.vme_event_quality_cuts = True
    
#     def apply_t5_event_quality_cuts(self):
#         self.t5_event_quality_cuts = True

#     def get_good_wcte_pmts(self):
#         config = self.get_configuration()
#         good_wcte_pmts = config["good_wcte_pmts"]
#         good_wcte_pmts_slots = good_wcte_pmts//100
#         good_wcte_pmts_positions = good_wcte_pmts%100
#         return good_wcte_pmts_slots, good_wcte_pmts_positions
            
#     def get_configuration(self):
#         tree = self.file['Configuration']
#         config = tree.arrays(library="ak", entry_start=0, entry_stop=1)
#         return config[0]
    
#     def get_data_quality_metrics(self):
#         tree = self.file['DataQualityMetrics']
#         data_quality_metrics = tree.arrays(library="ak", entry_start=0, entry_stop=1)
#         return data_quality_metrics[0]
    
#     def get_vme_analysis_scalar_results(self):
#         tree = self.file['vme_analysis_scalar_results']
#         vme_analysis_scalar_results = tree.arrays(library="ak", entry_start=0, entry_stop=1)
#         return vme_analysis_scalar_results[0]
    
#     def get_vme_analysis_run_info(self):
#         tree = self.file['vme_analysis_run_info']
#         vme_analysis_run_info = tree.arrays(library="ak", entry_start=0, entry_stop=1)
#         return vme_analysis_run_info[0]
    
#     def __enter__(self):
#         return self

#     def __exit__(self, *args):
#         self.file.close()
