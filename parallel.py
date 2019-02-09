"""
A parallel script to run on the cluster.
"""

import os
import sys
import numpy as np
import starrotate as sr
import starrotate.rotation_tools as rt
import pandas as pd

from multiprocessing import Pool

# Necessary to add cwd to path when script run
# by SLURM (since it executes a copy)
sys.path.append(os.getcwd())

def measure_prot(row):
    star = row[1]

    lcpath = "lightcurves/{}".format(str(int(star.kepid)).zfill(9))
    if not os.path.exists(lcpath):
        os.makedirs(lcpath)
    time, flux, flux_err = rt.download_light_curves(star.kepid, lcpath)

    # Measure rotation period
    rotate = sr.RotationModel(time, flux, flux_err, star.kepid)
    t0, dur, porb = None, None, None
    gp_stuff, ls_period, acf_period = rotate.measure_rotation_period(
        t0, dur, porb)
    gp_period, errp, errm, Q, Qerrp, Qerrm = gp_stuff

    print("gp_period = ", gp_period, "LS period = ", ls_period,
          "acf period = ", acf_period)

    # Save results
    star["gp_period"] = gp_period
    star["gp_period_errp"] = errp
    star["gp_period_errm"] = errm
    star["logQ"] = Q
    star["logQ_errp"] = Qerrp
    star["logQ_errm"] = Qerrm
    star["acf_period"] = acf_period
    star["ls_period"] = ls_period

    star.to_csv("{}_results.csv".format(str(int(star.kepid)).zfill(9)))


if __name__ == "__main__":

    # Load Stephen Kane's catalogue.
    full_df = pd.read_csv("kane_cks_tdmra_dr2.csv")
    df = full_df.drop_duplicates(subset="kepid")

    df = df.iloc[:24]
    assert len(df.kepid) == len(np.unique(df.kepid))

    p = Pool(24)
    p.map(measure_prot, df.iterrows())
