"""liquid_meme_backtest.py on 1h candles over 75 days (bigger moves per trade vs. the same cost).

Run from the repo root: python -u tools/liquid_meme_backtest_1h.py
"""

import os
import runpy
from pathlib import Path

os.environ.setdefault("BT_TF", "hour:1")
os.environ.setdefault("BT_DAYS", "75")
runpy.run_path(str(Path(__file__).with_name("liquid_meme_backtest.py")), run_name="__main__")
