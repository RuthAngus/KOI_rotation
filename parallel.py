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
    df = row[1]
    starname = str(int(df.kepid)).zfill(9)

    lcpath = "data/lightcurves/{}".format(starname)
    time, flux, flux_err = rt.download_light_curves(df.kepid, ".", lcpath)

    # Measure rotation period
    rotate = sr.RotationModel(time, flux, flux_err, starname, plot=True)
    t0, dur, porb = df.koi_time0, df.koi_duration, df.koi_period
    rotate.process_light_curve(t0, dur, porb)

    ls_period = rotate.LS_rotation()
    acf_period = rotate.ACF_rotation()
    gp_stuff = rotate.GP_rotation(init_period=ls_period)
    gp_period, errp, errm, Q, Qerrp, Qerrm = gp_stuff

    # Save results
    df["gp_period"] = gp_period
    df["gp_period_errp"] = errp
    df["gp_period_errm"] = errm
    df["logQ"] = Q
    df["logQ_errp"] = Qerrp
    df["logQ_errm"] = Qerrm
    df["acf_period"] = acf_period
    df["ls_period"] = ls_period

    df.to_csv("{}_results.csv".format(str(int(star.kepid)).zfill(9)))


if __name__ == "__main__":

    # Load Stephen Kane's catalogue.
    full_df = pd.read_csv("kane_cks_tdmra_dr2.csv")
    df = full_df.drop_duplicates(subset="kepid")

    df = df.iloc[:24]
    assert len(df.kepid) == len(np.unique(df.kepid))

    p = Pool(24)
    p.map(measure_prot, df.iterrows())
