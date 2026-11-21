"""Resource-bounded PDF text worker. Never follows embedded links or executes content."""

import json
import sys


def extract(path):
    import resource

    try:
        if sys.platform != "linux":
            raise ValueError()
        resource.setrlimit(resource.RLIMIT_AS, (512 * 1024 * 1024, 512 * 1024 * 1024))
    except (OSError, ValueError):
        pass
    from pypdf import PdfReader

    reader = PdfReader(path, strict=True)
    if reader.is_encrypted or len(reader.pages) > 50:
        raise ValueError("PDF需未加密且不超过50页")
    pages = []
    for index, page in enumerate(reader.pages, 1):
        pages.append({"page": index, "text": (page.extract_text() or "")[:40000]})
    return pages


if __name__ == "__main__":
    print(json.dumps(extract(sys.argv[1]), ensure_ascii=False))
