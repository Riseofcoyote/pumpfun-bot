import os, sys
sys.argv = [__file__, "1", "3"]
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import copy_wallets
copy_wallets.main(1, 3)
