#!/usr/bin/env python3

from pathlib import Path
from subprocess import run
from sys import stderr


SCRIPTS = (
    "5asec.py",
    "agriloja.py",
    "aldi.py",
    "auchan.py",
    "audika.py",
    "burgerking.py",
    "burgerranch.py",
    "cgd.py",
    "continente.py",
    "decathlon.py",
    "element.py",
    "espacocasa.py",
    "kidtokid.py",
    "lidl.py",
    "mcdonalds.py",
    "mercadona.py",
    "minisom.py",
    "novobanco.py",
    "pingodoce.py",
    "radiopopular.py",
    "recheio.py",
    "solinca.py",
    "spar.py",
    "staples.py",
    "starbucks.py",
    "turiscar.py",
    "washy.py",
    "wells.py",
    "worten.py",
)


if __name__ == "__main__":
    base_dir = Path(__file__).parent

    for script in SCRIPTS:
        result = run([f"./{script}"], capture_output=True, cwd=base_dir)
        if result.returncode != 0:
            print(f"---\nScript '{script}' failed with exit code {result.returncode}: {result.stderr.decode()}", file=stderr)
