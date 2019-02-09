import starrotate as sr
import starrotate.rotation_tools as rt
import pandas as pd

# load stephen kane's catalogue.
kane = pd.read_csv("kane_cks_tdmra_dr2.csv")

for row in kane.iterrows():
    print(row[0], "of", len(kane))
    star = row[1]

    lcpath = "/Users/rangus/.kplr/data/lightcurves/{}".format(
        str(int(star.kepid)).zfill(9))
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

    # header = False
    # if row[0] == 0:
    #     header = True
    # with open("kane_with_rotation_periods.csv", "a") as f:
    #     star.to_csv(f, header=header)
