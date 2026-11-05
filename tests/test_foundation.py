"""Baseline Foundation Tests for DemandLens."""

from pathlib import Path

def test_directory_structure():
    """Verify core project directories exist."""
    required_dirs = [
        Path("data/bronze"),
        Path("data/silver"),
        Path("data/gold"),
        Path("notebooks/exploration"),
        Path("src/demandlens/ingestion"),
        Path("src/demandlens/transformations"),
        Path("src/demandlens/quality"),
        Path("src/demandlens/analytics"),
        Path("src/demandlens/rag"),
        Path("src/demandlens/app"),
        Path("tests"),
        Path("scripts"),
        Path("config"),
        Path("docs"),
    ]
    for directory in required_dirs:
        assert directory.is_dir(), f"Expected directory missing: {directory}"

def test_essential_files_exist():
    """Verify essential foundation files exist."""
    required_files = [
        Path("requirements.txt"),
        Path(".gitignore"),
        Path(".env.example"),
        Path("README.md"),
        Path("scripts/test_pyspark.py"),
    ]
    for file_path in required_files:
        assert file_path.is_file(), f"Expected file missing: {file_path}"

