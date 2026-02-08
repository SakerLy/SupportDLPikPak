import os
import sys
import subprocess
import time

def install_requirements():
    if getattr(sys, 'frozen', False):
        return

    required = ["requests", "rich"]
    missing = []
    
    for lib in required:
        try:
            __import__(lib)
        except ImportError:
            missing.append(lib)
    
    if missing:
        print("="*60)
        print(" INSTALLING MISSING LIBRARIES...")
        print("="*60)
        
        for lib in missing:
            print(f"⏳ Installing: {lib}...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", lib])
                print(f"✓ Installed: {lib}")
            except subprocess.CalledProcessError:
                print(f"✖ Failed to install: {lib}. Please install manually.")
                sys.exit(1)
        
        print("-" * 60)
        print("✓ Done. Starting Tool...")
        time.sleep(1)

def main():
    install_requirements()
    
    try:
        from config.settings import Config
        from core.utils import CacheManager
        from ui.menu import Menu
        
        os.system("title PikPak Dowloader " if os.name == "nt" else "")
        CacheManager.init()
        Config.setup_dirs()
        Config.load_config()
        Menu().main_menu()
    except ImportError as e:
        print(f"Error: Could not import modules. Ensure you are running main.py from the root folder.\nDetails: {e}")
        input("Press Enter to exit...")

if __name__ == "__main__":
    main()