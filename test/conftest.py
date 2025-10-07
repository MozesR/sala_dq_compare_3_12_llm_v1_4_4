import sys, pathlib

def pytest_configure(config):
    here = pathlib.Path(__file__).resolve().parents[1]
    src = here / "src"
    if src.exists():
        sys.path.insert(0, str(src))
