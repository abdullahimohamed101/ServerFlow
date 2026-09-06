import sys

try:
    import yaml
except ImportError:
    print("pyyaml not available - skipping")
    sys.exit(0)

for path in sys.argv[1:]:
    with open(path, encoding="utf-8") as f:
        yaml.safe_load(f)
    print(f"OK {path}")

print("all YAML files parse")