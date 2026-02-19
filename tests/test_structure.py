"""
Quick test to validate project structure.
Run: python tests/test_structure.py
"""

from pathlib import Path

def test_structure():
    """Check that all required directories exist."""
    
    required_dirs = [
        'data/raw',
        'data/processed',
        'src/data',
        'src/utils',
        'notebooks',
        'tests',
    ]
    
    required_files = [
        'src/__init__.py',
        'src/data/__init__.py',
        'src/utils/__init__.py',
        'requirements.txt',
        '.gitignore',
    ]
    
    print("Testing project structure...")
    
    all_good = True
    
    # Check directories
    for dir_path in required_dirs:
        path = Path(dir_path)
        if path.exists():
            print(f"  ✓ {dir_path}")
        else:
            print(f"  ✗ {dir_path} - MISSING!")
            all_good = False
    
    # Check files
    for file_path in required_files:
        path = Path(file_path)
        if path.exists():
            print(f"  ✓ {file_path}")
        else:
            print(f"  ✗ {file_path} - MISSING!")
            all_good = False
    
    if all_good:
        print("\n✅ Project structure is correct!")
    else:
        print("\n❌ Some files/directories are missing. Check setup commands.")
    
    return all_good

if __name__ == "__main__":
    test_structure()