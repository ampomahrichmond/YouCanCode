"""Paper executor — PHASE 3 STUB.

Will accept order intents from the strategy, simulate fills against the
latest tick, and write Order/Fill/Position/TradeRun rows to the journal
exactly the same way the live executor will. This symmetry is important so
the journal-driven analytics behave identically in paper and live modes.
"""

from __future__ import annotations


class PaperExecutor:
    async def submit(self, *args, **kwargs) -> None:
        raise NotImplementedError("PaperExecutor.submit — phase 3")
