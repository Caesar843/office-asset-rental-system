from __future__ import annotations

import re


ASSET_ID_PATTERN = re.compile(r"^[A-Z]{2,}(?:-[A-Z0-9]+)+$")
PREFIX_PATTERN = re.compile(r"^(?:ASSET[_ -]?ID|ASSET|ID)\s*[:=]\s*(?P<value>.+)$", re.IGNORECASE)
SEGMENT_SPLIT_PATTERN = re.compile(r"[|;,]+")


def is_formal_asset_id(value: str) -> bool:
    if not isinstance(value, str):
        return False
    cleaned = value.strip().upper()
    if not ASSET_ID_PATTERN.fullmatch(cleaned):
        return False
    segments = cleaned.split("-")
    return any(any(character.isdigit() for character in segment) for segment in segments[1:])


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
            normalized = self._normalize_candidate(candidate)
            if normalized is not None and self._is_formal_asset_id(normalized):
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
            candidates.extend(self._split_segments(line))
            line_match = PREFIX_PATTERN.match(line)
            if line_match:
                candidates.append(line_match.group("value"))
                candidates.extend(self._split_segments(line_match.group("value")))
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

    def _split_segments(self, value: str) -> list[str]:
        return [segment.strip() for segment in SEGMENT_SPLIT_PATTERN.split(value) if segment.strip()]

    def _normalize_candidate(self, candidate: str) -> str | None:
        cleaned = self._strip_wrappers(candidate).strip().strip(".,;:|/\\").upper()
        if not cleaned:
            return None
        if self._is_formal_asset_id(cleaned):
            return cleaned

        underscore_normalized = re.sub(r"_+", "-", cleaned)
        underscore_normalized = re.sub(r"-+", "-", underscore_normalized).strip("-")
        if self._is_formal_asset_id(underscore_normalized):
            return underscore_normalized

        tokens = [token for token in re.split(r"[\s_]+", cleaned) if token]
        if len(tokens) < 2:
            return None
        if not re.fullmatch(r"[A-Z]{2,}", tokens[0]):
            return None
        if not all(re.fullmatch(r"[A-Z0-9]+", token) for token in tokens[1:]):
            return None
        if not all(any(character.isdigit() for character in token) for token in tokens[1:]):
            return None
        normalized = "-".join(tokens)
        if self._is_formal_asset_id(normalized):
            return normalized
        return None

    def _is_formal_asset_id(self, value: str) -> bool:
        return is_formal_asset_id(value)
