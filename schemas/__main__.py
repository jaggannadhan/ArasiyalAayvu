"""Package CLI entry point.

Run as::

    python -m schemas --list
    python -m schemas <collection> <path.json> [--strict]

Using ``python -m schemas`` (rather than ``python -m schemas.validate``) avoids
the runpy double-import RuntimeWarning, since ``__main__`` is not imported by the
package ``__init__``.
"""

from .validate import main

raise SystemExit(main())
