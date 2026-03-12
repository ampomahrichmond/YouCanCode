"""
MetaHarvest  ·  Entry Point
"""
import sys
import os

# Make sure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    from app.ui.app_window import MetaHarvestApp
    app = MetaHarvestApp()
    app.mainloop()

if __name__ == "__main__":
    main()
