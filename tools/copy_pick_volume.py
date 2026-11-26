import os, runpy
os.environ["PICK_SOURCE"] = "volume"
runpy.run_path(os.path.join(os.path.dirname(os.path.abspath(__file__)), "copy_pick.py"), run_name="__main__")
