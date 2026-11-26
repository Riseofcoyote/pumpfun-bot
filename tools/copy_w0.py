import os, sys
sys.argv = [__file__, "0", "3"]
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import copy_wallets
copy_wallets.main(0, 3)
