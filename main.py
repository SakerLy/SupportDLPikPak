import os
import sys
import subprocess
import time
from core.logger import init_logger, logger


def install_requirements():
    if getattr(sys, "frozen", False):
        return
    # tên module import -> tên gói pip
    required = {
        "aiohttp": "aiohttp",
        "aiofiles": "aiofiles",
        "requests": "requests",
        "rich": "rich",
        "aiohttp_socks": "aiohttp-socks",
    }
    missing = []
    for mod, pkg in required.items():
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)

    if missing:
        print("=" * 60)
        print(" INSTALLING MISSING LIBRARIES...")
        print("=" * 60)
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
        from config.settings import Config, APP_VERSION
        from core.cache import CacheManager
        from ui.menu import Menu

        logger.info("Version %s", APP_VERSION)

        os.system("title PikPak Downloader" if os.name == "nt" else "")
        CacheManager.init()
        Config.migrate_config()
        Config.setup_dirs()
        Menu().main_menu()
    except ImportError as e:
        logger.exception("Failed to import modules")
        print(
            f"Error: Could not import modules. Ensure you are running main.py from the root folder.\nDetails: {e}"
        )
        input("Press Enter to exit...")
    except Exception as e:
        logger.exception("Unhandled exception in main")
        print(f"An unexpected error occurred:\n{e}")
        input("Press Enter to exit...")


if __name__ == "__main__":
    main()
