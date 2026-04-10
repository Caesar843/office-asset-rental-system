from __future__ import annotations

import re


ASSET_ID_PATTERN = re.compile(r"^[A-Z]{2,}(?:-[A-Z0-9]+)+$")
PREFIX_PATTERN = re.compile(r"^(?:ASSET[_ -]?ID|ASSET|ID)\s*[:=]\s*(?P<value>.+)$", re.IGNORECASE)


class AssetIdParser:
    """Conservative asset_id extractor.

    This parser intentionally performs only limited cleanup so the vision module
    does not invent half-valid asset IDs.
    """

    def parse(self, raw_text: str) -> str | None:
        cleaned = self._clean_text(raw_text)
        if not cleaned:
            return None

        candidates: list[str] = []
        for candidate in self._candidate_strings(cleaned):
            normalized = candidate.strip().upper()
            if ASSET_ID_PATTERN.fullmatch(normalized):
                candidates.append(normalized)

        unique_candidates = list(dict.fromkeys(candidates))
        if len(unique_candidates) != 1:
            return None
        return unique_candidates[0]

    def _candidate_strings(self, cleaned_text: str) -> list[str]:
        candidates = [cleaned_text]
        label_match = PREFIX_PATTERN.match(cleaned_text)
        if label_match:
            candidates.append(label_match.group("value"))

        for line in cleaned_text.splitlines():
            line = line.strip()
            if not line:
                continue
            candidates.append(line)
            line_match = PREFIX_PATTERN.match(line)
            if line_match:
                candidates.append(line_match.group("value"))
            candidates.append(self._strip_wrappers(line))
        return candidates

    def _clean_text(self, raw_text: str) -> str:
        if not isinstance(raw_text, str):
            return ""
        cleaned = raw_text.replace("\r", "\n").replace("\x00", "").strip()
        cleaned = "\n".join(line.strip() for line in cleaned.split("\n") if line.strip())
        return self._strip_wrappers(cleaned)

    def _strip_wrappers(self, value: str) -> str:
        cleaned = value.strip().strip("\"'`")
        changed = True
        while changed and len(cleaned) >= 2:
            changed = False
            for left, right in (("[", "]"), ("(", ")"), ("{", "}"), ("<", ">")):
                if cleaned.startswith(left) and cleaned.endswith(right):
                    cleaned = cleaned[1:-1].strip()
                    changed = True
        return cleaned
