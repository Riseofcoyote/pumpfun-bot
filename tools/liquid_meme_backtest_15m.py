"""liquid_meme_backtest.py on 15m candles over 30 days (bigger moves per trade vs. the same cost).

Run from the repo root: python -u tools/liquid_meme_backtest_15m.py
"""

import os
import runpy
from pathlib import Path

os.environ.setdefault("BT_TF", "minute:15")
os.environ.setdefault("BT_DAYS", "30")
runpy.run_path(str(Path(__file__).with_name("liquid_meme_backtest.py")), run_name="__main__")
