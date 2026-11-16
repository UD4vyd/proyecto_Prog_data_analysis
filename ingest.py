"""Ingestion entry point. Workshop 1 only defines the interface; the loaders arrive in Workshop 2."""
import argparse


def main():
    parser = argparse.ArgumentParser(description="Extreme weather monitoring (Texas) ingestion")
    parser.add_argument("--mode", choices=["historical", "incremental", "status"], required=True)
    args = parser.parse_args()
    print(f"Mode '{args.mode}' selected. Ingestion is implemented in Workshop 2.")


if __name__ == "__main__":
    main()
