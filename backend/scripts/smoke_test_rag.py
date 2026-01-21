from __future__ import annotations

import numpy as np

from app.rag.store import get_rag_store


def main() -> None:
    store = get_rag_store()
    has_index = store.has_data()

    if not has_index:
        query = np.zeros((384,), dtype="float32")
        results = store.search(query, top_k=5)
        store.close()
        print("ok: empty index handled") if results == [] else print("unexpected results")
        return

    store.close()
    print("ok: index exists")


if __name__ == "__main__":
    main()
