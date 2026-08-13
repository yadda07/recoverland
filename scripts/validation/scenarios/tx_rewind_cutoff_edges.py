"""tx_rewind_cutoff_edges - RecoverLand validation runtime.

Invariant: I-9 (la fenetre de lecture doit contenir exactement ce que
l'utilisateur a demande).
Sujet: la BORNE. Date pile sur un evenement, a la seconde puis a la
microseconde, date future, date anterieure a tout l'historique, date vide,
date incoherente.

Le mecanisme
------------
Le cutoff n'est jamais compare comme une date : c'est une comparaison de
CHAINES en SQL (``created_at >= ?``). Deux formats se rencontrent :

    journal  : datetime.now(timezone.utc).isoformat()
               -> "2026-08-12T09:00:00.123456+00:00"
               -> "2026-08-12T09:00:00+00:00" quand la microseconde vaut 0
    dialogue : cutoff_dt.toUTC().toString("yyyy-MM-ddTHH:mm:ss")
               -> "2026-08-12T09:00:00"  (ni microseconde, ni decalage)

L'ordre lexicographique fait le bon travail dans le sens de la production :
la chaine du dialogue est un PREFIXE de celle du journal, donc tout
evenement de la meme seconde est plus grand et rentre dans la fenetre
inclusive. Le scenario le verrouille au lieu de l'esperer.

Il ne le fait plus des que la borne est plus PRECISE que l'evenement :
".000000" (0x2E) est superieur a "+00:00" (0x2B), donc l'evenement ecrit
sur une seconde pleine passe pour ANTERIEUR a une borne exprimee a la
microseconde sur le meme instant.

Cote validation, `restore_contracts.validate_cutoff` ne verifie qu'un type
et une longueur (>= 10 caracteres) : n'importe quelle chaine assez longue
est acceptee comme une date. Et le depot de lecture ne valide rien du tout :
une borne vide donne ``created_at >= ''``, vrai pour toutes les lignes.

Scenario d'echec concret pour l'utilisateur
-------------------------------------------
C4 - l'utilisateur demande "reviens juste avant cet evenement" et la borne
est construite depuis le created_at complet d'un autre evenement. Un
evenement tombe sur une seconde pleine : il reste HORS de la fenetre alors
qu'il est exactement sur la borne. Le rewind le laisse en place et se
declare reussi.

C7/C8 - une borne vide ou incoherente atteint le depot de lecture sans
controle : vide, elle selectionne TOUT le journal (rembobinage depuis
l'origine) ; incoherente mais assez longue, elle est declaree valide et ne
selectionne rien, l'utilisateur lit "aucun evenement apres cette date" pour
une date qui n'existe pas. Le preflight rattrape le cas vide avant toute
ecriture ; c'est le seul filet.

Plan du scenario (journal SQLite reel, zero objet QGIS), ds=tx_ce_main :
    E_before  UPDATE  t0-0.5 s
    E_exact   UPDATE  t0 pile, seconde pleine ("...T09:00:00+00:00")
    E_us      UPDATE  t0 + 1 microseconde
    E_late    UPDATE  t0 + 10.75 s
    T         trace   t0 + 30 s, reference E_exact
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_rewind_cutoff_edges"
INVARIANT = "I-9"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_PROJECT_FP = "tx_ce_project"
_DS = "tx_ce_main"
_FP = "fid:73001"
_IDENTITY = json.dumps({"fid": 73001})
_FIELDS = json.dumps([{"name": "code", "type": "string"}])


def _insert(conn, ts, op, attrs, restored_from=None) -> int:
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    sql = (
        "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
        + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )
    row = (
        _PROJECT_FP, _DS, "lyr_ce", "ce_layer", "ogr",
        _IDENTITY, op, json.dumps(attrs, ensure_ascii=False),
        None, "Point", "EPSG:4326", _FIELDS,
        "tester", None, ts, restored_from, _FP, 2, None, None,
    )
    return conn.execute(sql, row).lastrowid


def _delta(old, new):
    return {"changed_only": {"code": {"old": old, "new": new}}}


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)

    stamps = {
        # isoformat() omet la microseconde quand elle vaut zero : c'est le
        # cas qui casse la comparaison de chaines.
        "exact": t0.isoformat(),
        "before": (t0 - timedelta(milliseconds=500)).isoformat(),
        "us": (t0 + timedelta(microseconds=1)).isoformat(),
        "late": (t0 + timedelta(seconds=10, milliseconds=750)).isoformat(),
        "trace": (t0 + timedelta(seconds=30)).isoformat(),
    }
    eids = {
        "before": _insert(conn, stamps["before"], "UPDATE", _delta("O", "P")),
        "exact": _insert(conn, stamps["exact"], "UPDATE", _delta("A", "B")),
    }
    eids["us"] = _insert(conn, stamps["us"], "UPDATE", _delta("B", "C"))
    eids["late"] = _insert(conn, stamps["late"], "UPDATE", _delta("C", "D"))
    eids["trace"] = _insert(
        conn, stamps["trace"], "UPDATE",
        {"_restore_ref": eids["exact"]}, restored_from=eids["exact"])
    conn.commit()

    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    ctx.data["stamps"] = stamps
    ctx.data["eids"] = eids

    flog(
        f"tx_rewind_cutoff_edges setup: trace_id={ctx.trace_id} "
        f"eids={eids} exact_stamp={stamps['exact']}",
        "INFO",
    )


def _probe(ctx, value, inclusive=True, cutoff_type=None):
    """Lecture + validation + preflight pour une valeur de borne donnee."""
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import (
        RestoreCutoff, CutoffType, validate_cutoff,
    )
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.restore_planner import plan_temporal_restore, preflight_check

    ctype = cutoff_type or CutoffType.BY_DATE
    cutoff = RestoreCutoff(ctype, value, inclusive=inclusive)
    try:
        events = fetch_events_after_cutoff(
            ctx.data["conn"], _DS, cutoff, trace_id=ctx.trace_id)
        fetch_error = None
    except Exception as exc:  # noqa: BLE001 - une borne hostile ne doit pas exploser
        events, fetch_error = [], repr(exc)
    active, _stats = collapse_rewind_events_with_stats(events)
    plan = plan_temporal_restore(active, _DS, "ce_layer", cutoff)
    report = preflight_check(plan)
    return {
        "fetched": sorted(e.event_id for e in events),
        "active": sorted(e.event_id for e in active),
        "fetch_error": fetch_error,
        "validation": validate_cutoff(cutoff),
        "verdict": report.verdict.value,
        "blocking": list(report.blocking_reasons),
    }


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_contracts import CutoffType, RestoreCutoff
    from recoverland.core.restore_planner import check_retention_coverage
    from recoverland.core.event_stream_repository import get_oldest_event_date

    t0 = ctx.data["t0"]
    stamps = ctx.data["stamps"]

    flog(f"tx_rewind_cutoff_edges run start: trace_id={ctx.trace_id}", "INFO")

    # C1 : le format produit par le dialogue (seconde, sans decalage).
    ctx.data["c1_dialog_format"] = _probe(ctx, "2026-08-12T09:00:00")
    # C2 : borne = created_at complet d'un evenement, inclusive.
    ctx.data["c2_exact_inclusive"] = _probe(ctx, stamps["exact"])
    # C3 : la meme, exclusive.
    ctx.data["c3_exact_exclusive"] = _probe(ctx, stamps["exact"], inclusive=False)
    # C4 : meme instant, mais exprime a la microseconde.
    ctx.data["c4_microsecond_form"] = _probe(ctx, "2026-08-12T09:00:00.000000+00:00")
    # C5 : borne dans le futur.
    ctx.data["c5_future"] = _probe(ctx, (t0 + timedelta(days=1)).isoformat())
    # C6 : borne anterieure a tout l'historique.
    ctx.data["c6_before_history"] = _probe(ctx, (t0 - timedelta(days=365)).isoformat())
    # C7 : borne vide.
    ctx.data["c7_empty"] = _probe(ctx, "")
    # C8 : borne incoherente mais assez longue pour passer la validation.
    ctx.data["c8_garbage"] = _probe(ctx, "pas-une-date-du-tout")
    # C9 : borne None.
    ctx.data["c9_none"] = _probe(ctx, None)
    # C10 : borne par event_id, mais avec une valeur texte.
    ctx.data["c10_eventid_text"] = _probe(
        ctx, "douze", cutoff_type=CutoffType.BY_EVENT_ID)

    oldest = get_oldest_event_date(ctx.data["conn"], _DS)
    ctx.data["oldest"] = oldest
    ctx.data["c6_retention"] = check_retention_coverage(
        RestoreCutoff(CutoffType.BY_DATE, (t0 - timedelta(days=365)).isoformat(),
                      inclusive=True),
        oldest)
    ctx.data["c1_retention"] = check_retention_coverage(
        RestoreCutoff(CutoffType.BY_DATE, "2026-08-12T09:00:00", inclusive=True),
        oldest)

    flog(
        f"tx_rewind_cutoff_edges run end: trace_id={ctx.trace_id} "
        f"c1={ctx.data['c1_dialog_format']['fetched']} "
        f"c4={ctx.data['c4_microsecond_form']['fetched']} "
        f"c7={ctx.data['c7_empty']['fetched']} "
        f"c8={ctx.data['c8_garbage']['fetched']}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    eids = ctx.data["eids"]
    e_before, e_exact = eids["before"], eids["exact"]
    e_us, e_late, e_trace = eids["us"], eids["late"], eids["trace"]

    c1 = ctx.data.get("c1_dialog_format") or {}
    c2 = ctx.data.get("c2_exact_inclusive") or {}
    c3 = ctx.data.get("c3_exact_exclusive") or {}
    c4 = ctx.data.get("c4_microsecond_form") or {}
    c5 = ctx.data.get("c5_future") or {}
    c6 = ctx.data.get("c6_before_history") or {}
    c7 = ctx.data.get("c7_empty") or {}
    c8 = ctx.data.get("c8_garbage") or {}
    c9 = ctx.data.get("c9_none") or {}
    c10 = ctx.data.get("c10_eventid_text") or {}

    # ===== C1 : le format reellement produit par l'interface ============
    out.append((
        "dialog_second_precision_takes_the_whole_second",
        c1.get("fetched") == sorted([e_exact, e_us, e_late, e_trace]),
        f"fenetre={c1.get('fetched')} attendu="
        f"{sorted([e_exact, e_us, e_late, e_trace])} : le dialogue envoie une "
        f"borne a la seconde sans decalage horaire. Tout evenement de cette "
        f"seconde doit entrer dans la fenetre, y compris la trace qui "
        f"reference l'evenement pile sur la borne, sinon la memoire du dedup "
        f"est perdue",
    ))
    out.append((
        "dialog_format_excludes_what_is_before",
        e_before not in (c1.get("fetched") or []),
        f"fenetre={c1.get('fetched')} ne doit pas contenir {e_before} "
        f"(evenement d'avant la borne) : le rewind annulerait une edition "
        f"anterieure a la date demandee",
    ))

    # ===== C2 / C3 : borne pile sur un evenement ========================
    out.append((
        "exact_stamp_inclusive_keeps_the_event",
        e_exact in (c2.get("fetched") or []),
        f"fenetre={c2.get('fetched')} doit contenir {e_exact} : la borne vaut "
        f"exactement le created_at de cet evenement et la fenetre est "
        f"inclusive",
    ))
    out.append((
        "exact_stamp_exclusive_drops_the_event",
        e_exact not in (c3.get("fetched") or [])
        and e_us in (c3.get("fetched") or []),
        f"fenetre exclusive={c3.get('fetched')} : l'evenement pile sur la borne "
        f"doit sortir et celui d'une microseconde plus tard doit rester, sinon "
        f"les deux modes de borne designent la meme chose",
    ))

    # ===== C4 : le meme instant, ecrit a la microseconde ================
    out.append((
        "same_instant_written_with_microseconds_selects_the_same_events",
        c4.get("fetched") == c2.get("fetched"),
        f"fenetre microseconde={c4.get('fetched')} vs fenetre seconde="
        f"{c2.get('fetched')} : les deux bornes designent le MEME instant "
        f"(09:00:00.000000 UTC). Ecrite avec les microsecondes, la borne "
        f"laisse dehors l'evenement {e_exact} tombe sur une seconde pleine, "
        f"ainsi que la trace {e_trace} qui le reference : l'edition n'est pas "
        f"annulee et la memoire de dedup est perdue, sans aucun message",
    ))

    # ===== C5 : borne dans le futur =====================================
    out.append((
        "future_cutoff_reads_nothing",
        (c5.get("fetched") or []) == [],
        f"fenetre={c5.get('fetched')} attendu=[] : aucun evenement n'est "
        f"posterieur a une date future",
    ))
    out.append((
        "future_cutoff_is_refused_explicitly",
        c5.get("verdict") == "blocked"
        and any("No actions" in b for b in (c5.get("blocking") or [])),
        f"verdict={c5.get('verdict')!r} motifs={c5.get('blocking')} : le "
        f"preflight doit refuser avec un motif lisible plutot que de lancer "
        f"une restauration vide",
    ))

    # ===== C6 : borne anterieure a tout l'historique ====================
    out.append((
        "cutoff_before_history_takes_everything",
        c6.get("fetched") == sorted([e_before, e_exact, e_us, e_late, e_trace]),
        f"fenetre={c6.get('fetched')} attendu=tout le journal : une borne "
        f"anterieure au premier evenement doit tout selectionner",
    ))
    out.append((
        "cutoff_before_history_warns_about_the_gap",
        ctx.data.get("c6_retention") is not None
        and ctx.data.get("c1_retention") is None,
        f"couverture retention (borne ancienne)="
        f"{ctx.data.get('c6_retention')!r} / (borne dans l'historique)="
        f"{ctx.data.get('c1_retention')!r} oldest={ctx.data.get('oldest')!r} : "
        f"l'utilisateur doit etre prevenu que l'historique ne remonte pas "
        f"jusqu'a la date demandee, sinon il croit avoir retrouve un etat que "
        f"le journal ne connait pas",
    ))

    # ===== C7 : borne vide ==============================================
    out.append((
        "empty_cutoff_is_rejected_before_any_write",
        c7.get("verdict") == "blocked"
        and any("Invalid cutoff" in b for b in (c7.get("blocking") or [])),
        f"verdict={c7.get('verdict')!r} motifs={c7.get('blocking')} : le "
        f"preflight est le dernier filet avant ecriture, il doit refuser une "
        f"borne vide",
    ))
    out.append((
        "empty_cutoff_does_not_select_the_whole_journal",
        (c7.get("fetched") or []) == [],
        f"fenetre={c7.get('fetched')} attendu=[] : ``created_at >= ''`` est "
        f"vrai pour toutes les lignes. Le depot de lecture ne valide pas la "
        f"borne, donc une borne vide vaut 'depuis l'origine des temps'. Tout "
        f"appelant qui lit sans passer par le preflight rembobine le journal "
        f"entier",
    ))

    # ===== C8 : borne incoherente =======================================
    out.append((
        "garbage_cutoff_is_rejected_by_validation",
        c8.get("validation") is not None,
        f"validate_cutoff={c8.get('validation')!r} : la validation ne teste "
        f"qu'un type et une longueur >= 10, donc 'pas-une-date-du-tout' est "
        f"declaree valide. Fenetre lue={c8.get('fetched')} : l'utilisateur "
        f"recoit 'aucun evenement apres cette date' pour une date qui n'existe "
        f"pas, au lieu d'une erreur de saisie",
    ))
    out.append((
        "garbage_cutoff_never_writes_anything",
        c8.get("verdict") == "blocked",
        f"verdict={c8.get('verdict')!r} motifs={c8.get('blocking')} : quoi "
        f"qu'il arrive, une borne incoherente ne doit produire aucune ecriture",
    ))

    # ===== C9 / C10 : types hostiles ====================================
    out.append((
        "none_cutoff_survives_without_crashing",
        c9.get("fetch_error") is None and (c9.get("fetched") or []) == []
        and c9.get("verdict") == "blocked",
        f"erreur={c9.get('fetch_error')!r} fenetre={c9.get('fetched')} "
        f"verdict={c9.get('verdict')!r} : une borne absente doit donner une "
        f"fenetre vide et un refus, pas une exception qui remonte jusqu'a "
        f"l'interface",
    ))
    out.append((
        "event_id_cutoff_rejects_a_text_value",
        c10.get("validation") is not None and c10.get("verdict") == "blocked"
        and c10.get("fetch_error") is None,
        f"validation={c10.get('validation')!r} verdict={c10.get('verdict')!r} "
        f"erreur={c10.get('fetch_error')!r} : une borne par event_id doit "
        f"exiger un entier",
    ))

    # ===== Propagation de trace =========================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_rewind_cutoff_edges.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
