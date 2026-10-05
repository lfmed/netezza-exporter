"""Incrementa a versão em 0.001 (0.990 -> 0.991) e abre a entrada no CHANGELOG.

Uso: .venv/bin/python scripts/bump_version.py "descrição do ajuste"
"""
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

root = Path(__file__).resolve().parents[1]
vfile = root / "src" / "VERSION"
new = f"{Decimal(vfile.read_text().strip()) + Decimal('0.001'):.3f}"
vfile.write_text(new + "\n")
desc = " ".join(sys.argv[1:]) or "ajuste"
log = root / "CHANGELOG.md"
head, _, rest = log.read_text().partition("\n## ")
log.write_text(f"{head}\n## {new} — {date.today().isoformat()}\n- {desc}\n\n## {rest}")
print(new)
