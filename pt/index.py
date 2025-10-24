#!/usr/bin/env python3

import sys
from pathlib import Path
from subprocess import TimeoutExpired, run
from traceback import format_exception


SCRIPTS = (
    "5asec.py",
    "agriloja.py",
    "aldi.py",
    "amanhecer.py",
    "auchan.py",
    "audika.py",
    "bricomarche.py",
    "burgerking.py",
    "burgerranch.py",
    "celeiro.py",
    "cgd.py",
    "chip7.py",
    "continente.py",
    "decathlon.py",
    "element.py",
    "espacocasa.py",
    "froiz.py",
    "jysk.py",
    "kidtokid.py",
    "lidl.py",
    "mcdonalds.py",
    "mercadona.py",
    "meusuper.py",
    "minisom.py",
    "pingodoce.py",
    "radiopopular.py",
    "recheio.py",
    "remax.py",
    "roady.py",
    "santander.py",
    "solinca.py",
    "spar.py",
    "staples.py",
    "starbucks.py",
    "synlab.py",
    "telpark.py",
    "turiscar.py",
    "washy.py",
    "wells.py",
    "widex.py",
    "worten.py",
)


if __name__ == "__main__":
    base_dir = Path(__file__).parent

    for script in SCRIPTS:
        try:
            result = run([sys.executable, script], check=False, capture_output=True, cwd=base_dir, text=True, timeout=600)  # noqa: S603
            exit_code = result.returncode
            output = result.stderr
        except TimeoutExpired as e:
            exit_code = "<timeout>"
            output = "".join(format_exception(e)).rstrip()
        if exit_code != 0:
            print(f"---\nScript '{script}' failed with exit code {exit_code}: {output}", file=sys.stderr)
