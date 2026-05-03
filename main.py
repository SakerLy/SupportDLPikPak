import os
import sys
import subprocess
import time
from core.logger import init_logger, logger

def install_requirements():
    if getattr(sys, 'frozen', False): return
    required = ["aiohttp", "aiofiles", "requests", "rich"]
    missing = []
    for lib in required:
        try: __import__(lib)
        except ImportError: missing.append(lib)

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
    init_logger()
    logger.info("Starting PikPak Downloader")
    try:
        from config.settings import Config
        from core.utils import CacheManager
        from ui.menu import Menu

        os.system("title PikPak Downloader" if os.name == "nt" else "")
        CacheManager.init()
        Config.setup_dirs()
        Config.migrate_config()
        Menu().main_menu()
    except ImportError as e:
        logger.exception("Failed to import modules")
        print(f"Error: Could not import modules. Ensure you are running main.py from the root folder.\nDetails: {e}")
        input("Press Enter to exit...")
    except Exception as e:
        logger.exception("Unhandled exception in main")
        print(f"An unexpected error occurred:\n{e}")
        input("Press Enter to exit...")

if __name__ == "__main__":
    main()