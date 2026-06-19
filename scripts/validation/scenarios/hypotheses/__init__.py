"""Runtime non-regression scenarios for SESSION_REWIND chapter 17 hypotheses.

Each module emits a final log line:
    hypothesis_h_<id>: status=VALIDATED|FALSIFIED|UNREPRODUCED reason=...

Verdict semantics:
    - VALIDATED   : the hypothesis (a real-world risk) is reproduced.
    - FALSIFIED   : the hypothesis is not reproducible; the codebase is
                    robust against it.
    - UNREPRODUCED: the scenario could not exercise the path (missing
                    fixture, environment skip).

BL-RW-P3-19 / CR-10.
"""
