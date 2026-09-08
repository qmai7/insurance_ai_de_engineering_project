"""
Promote a registered model version — the manual gate.

CLAUDE.md leaves model promotion as an open decision and says to implement the
manual path first, so this is a separate command rather than a step in
`ml/train.py`. Training always registers; promoting is a decision someone makes
after reading the metrics.

    python -m ml.promote --version 3
    python -m ml.promote --show

What "promotion" is, concretely: moving the `production` alias to a version.
MLflow 3 removed model stages, and an alias is better suited anyway — it moves
atomically, so there is never a moment where serving resolves to nothing, and
the same primitive supports §13's champion/challenger by adding a second alias.

The automated alternative — a registry watcher that promotes any version beating
the incumbent on validation PR-AUC — is documented as a stretch goal in
`docs/ml.md` rather than built, because auto-promotion needs a guard this project
does not have yet: without ground truth at serving time, "better on validation"
is not evidence a model is better in production.
"""

from __future__ import annotations

import argparse
import sys

from . import config
from .services import ModelRegistryService


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--version", help="model version to promote")
    parser.add_argument("--alias", default=config.PRODUCTION_ALIAS)
    parser.add_argument("--show", action="store_true", help="print the current alias target")
    args = parser.parse_args(argv)

    registry = ModelRegistryService()
    current = registry.current_alias_version(args.alias)

    if args.show or not args.version:
        print(f"{config.REGISTERED_MODEL} @{args.alias} -> {current or 'nothing'}")
        if current:
            # The storage path, not the models:/ URI — this is the value model-server
            # needs, so printing it here makes the serving config copy-pasteable.
            print(f"  artifact location: {registry.aliased_model_location(args.alias)}")
        return 0 if args.show else 1

    if current == args.version:
        print(f"@{args.alias} already points at v{args.version}; nothing to do")
        return 0

    registry.promote(args.version, args.alias)
    print(f"{config.REGISTERED_MODEL} @{args.alias}: "
          f"{current or 'nothing'} -> v{args.version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
