import uproot

def save_histograms(hist_dict, filename, add_offset=False):
    with uproot.recreate(filename) as f:
        # Save all available histograms to disk
        for channel, histogram in hist_dict.items():
            # Optionally add minimal offset to avoid completely empty bins
            # (useful for the ML validation variables that would need binning adjustment
            # to avoid those)
            if add_offset:
                histogram += 1e-6
                # Reference count for empty histogram with floating point math tolerance
                empty_hist_yield = histogram.axes[0].size * (1e-6) * 1.01
            else:
                empty_hist_yield = 0

            for sample in histogram.axes[1]:
                for variation in histogram[:, sample, :].axes[1]:
                    variation_string = "" if variation == "nominal" else f"_{variation}"
                    current_1d_hist = histogram[:, sample, variation]

                    if sum(current_1d_hist.values()) > empty_hist_yield:
                        # Only save histograms containing events
                        # Many combinations are not used (e.g. ME var for W+jets)
                        f[f"{channel}_{sample}{variation_string}"] = current_1d_hist

            # Add pseudodata histogram if any input to it is available
            ttbar_me_var = None
            ttbar_ps_var = None
            wjets_nominal = None

            try:
                ttbar_me_var = histogram[:, "ttbar", "ME_var"]
            except KeyError:
                pass

            try:
                ttbar_ps_var = histogram[:, "ttbar", "PS_var"]
            except KeyError:
                pass

            try:
                wjets_nominal = histogram[:, "wjets", "nominal"]
            except KeyError:
                pass

            # Skip pseudodata if no required inputs are available
            if ttbar_me_var is None and ttbar_ps_var is None and wjets_nominal is None:
                continue

            # Calculate the pseudodata histogram
            pseudodata = 0
            count = 0
            if ttbar_me_var is not None and sum(ttbar_me_var.values()) > empty_hist_yield:
                pseudodata += ttbar_me_var
                count += 1

            if ttbar_ps_var is not None and sum(ttbar_ps_var.values()) > empty_hist_yield:
                pseudodata += ttbar_ps_var
                count += 1

            if wjets_nominal is not None and sum(wjets_nominal.values()) > empty_hist_yield:
                pseudodata += wjets_nominal
                count += 1

            if count > 0:
                pseudodata /= count
                f[f"{channel}_pseudodata"] = pseudodata
