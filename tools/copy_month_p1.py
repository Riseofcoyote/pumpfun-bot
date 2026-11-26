import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import copy_month
d = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
w = json.load(open(os.path.join(d, "month_prior.json")))
json.dump(w[1::2], open(os.path.join(d, "month_prior_1.json"), "w"))
copy_month.run("month_prior_1")
