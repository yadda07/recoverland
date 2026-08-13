"""tx_life_watchdog_log_disk - RecoverLand validation runtime.

Invariant: I-WATCHDOG (les deux surveillances du plugin — le journal de
debogage et l'espace disque — doivent rester lisibles et honnetes sous
contrainte).

Pourquoi ces deux sujets dans le meme fichier
---------------------------------------------
Ce sont les deux seuls capteurs que RecoverLand possede sur son propre
etat. Le journal de debogage est la seule preuve exploitable apres un
incident chez l'utilisateur ; la surveillance disque est le seul
mecanisme qui coupe l'enregistrement avant que le disque ne rende
l'ecriture impossible. Si l'un ment, le diagnostic est mort ; si l'autre
ment, la donnee est morte.

Partie A — le format cle=valeur
-------------------------------
`core.logger.flog_kv` protege les valeurs avec `_format_kv_value` :
espace, `=`, guillemet, tabulation et retour a la ligne declenchent un
encadrement par des quotes, pour que `scripts/validation/parse_log._KV_RE`
puisse retrouver la valeur d'origine.

Deux angles morts :

1. Le retour a la ligne est encadre par des quotes mais reste ecrit
   litteralement dans le fichier. `RotatingFileHandler` produit alors
   DEUX lignes physiques. `parse_log._LINE_RE` ne reconnait plus la
   seconde (pas d'horodatage) et la jette ; la premiere se termine par
   une quote non fermee, donc la valeur recuperee est tronquee. Une
   valeur d'attribut multi-ligne (commentaire, adresse, message
   d'exception) suffit a rendre la ligne incomprehensible.

2. `core.observability` n'utilise PAS `_format_kv_value`.
   `log_cycle_summary` construit ses paires avec un simple
   `f"{key}={stats[key]}"`, et `assert_invariant` fait pareil pour son
   contexte. Une valeur contenant une espace decale toutes les paires
   suivantes : `CYCLE_SUMMARY` est justement la ligne que l'on relit en
   priorite apres un REWIND rate.

Partie B — la surveillance disque
---------------------------------
`RecoverPlugin._start_disk_monitor` interroge `check_disk_for_path`
toutes les 5 minutes et, si `is_critical`, desactive le suivi et ecrit
`RecoverLand/tracking_enabled = False` dans QgsSettings.

Trois angles morts :

1. Volume injoignable (partage reseau debranche, lettre de lecteur
   disparue). `_find_existing_parent` ne trouve rien, la fonction rend
   `DiskStatus(0, 0, "", is_low=False, is_critical=False)` : zero octet
   libre est classe « sain ». La surveillance ne se declenchera jamais.

2. Branche `OSError` (peripherique non pret, timeout reseau) : la
   fonction rend `is_low=True, is_critical=False` avec 0 octet libre.
   Zero octet est en dessous du seuil critique de 100 Mo, mais la
   contradiction fait que le suivi n'est pas coupe.

3. Le seuil `is_low` (500 Mo) ne sert a rien. `_check` ne regarde que
   `is_critical`, donc le message « Espace disque faible » construit par
   `format_disk_message` n'est emis nulle part : l'utilisateur n'a
   jamais d'avertissement precoce, seulement la coupure brutale. Et
   quand l'espace revient, rien ne rallume le suivi ni ne previent que
   l'enregistrement est toujours coupe.

Ce que ce scenario torture (aucun objet QGIS, aucun fichier de projet) :
    A1  8 threads x 60 lignes flog_kv concurrentes
    A2  aller-retour de 9 valeurs speciales dans flog_kv
    A3  message enorme (300 Ko sur une seule ligne)
    A4  CycleStats sous 8 threads + finalize() idempotent
    A5  CYCLE_SUMMARY et INVARIANT_VIOLATED avec des valeurs a espaces
    B1  disque a 50 Mo   -> coupure annoncee
    B2  retour a 20 Go   -> qui rallume le suivi ?
    B3  volume injoignable
    B4  branche OSError
    B5  le seuil « faible » sert-il a quelque chose ?
"""
from __future__ import annotations

import os
import shutil
import tempfile
import threading
from pathlib import Path

SCENARIO_ID = "tx_life_watchdog_log_disk"
INVARIANT = "I-WATCHDOG"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_MB = 1024 * 1024
_GB = 1024 * 1024 * 1024

# Valeurs speciales injectees dans flog_kv puis relues via parse_log.
_SPECIAL_VALUES = {
    "simple": "parcelles",
    "espace": "couche des parcelles",
    "egal": "a=b",
    "double": 'il a dit "non"',
    "simple_quote": "l'entite",
    "les_deux": 'l\'entite dite "pont"',
    "accents": "reseau d'assainissement - zone amenagee",
    "vide": "",
    "tabulation": "avant\tapres",
    "retour_ligne": "avant\napres",
}


class _FakeUsage:
    def __init__(self, free: int):
        self.total = 500 * _GB
        self.free = free
        self.used = self.total - free


class _FakeShutil:
    """Remplace `shutil` dans disk_monitor pour simuler un volume."""

    free = 0

    @classmethod
    def disk_usage(cls, _path):
        return _FakeUsage(cls.free)


class _RaisingShutil:
    @staticmethod
    def disk_usage(_path):
        raise OSError(21, "Le peripherique n'est pas pret")


def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_watchdog_")
    journal_dir = os.path.join(tmpdir, "j")
    os.makedirs(journal_dir, exist_ok=True)
    ctx.data["tmpdir"] = tmpdir
    ctx.data["journal_path"] = os.path.join(journal_dir, "recoverland_audit.sqlite")
    with open(ctx.data["journal_path"], "wb") as fh:
        fh.write(b"")

    flog(
        f"tx_life_watchdog_log_disk setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog, flog_kv
    from recoverland.core import disk_monitor as dm
    from recoverland.core.health_monitor import check_disk_space
    from recoverland.core.observability import (
        CycleStats, log_cycle_summary, assert_invariant,
    )

    flog(f"tx_life_watchdog_log_disk run start: trace_id={ctx.trace_id}", "INFO")

    # ===== A1 : 8 threads ecrivent en meme temps ========================
    n_threads, n_lines = 8, 60
    marker = "TX_WATCH_CONC"

    def _writer(tid: int) -> None:
        for i in range(n_lines):
            flog_kv(
                "INFO", marker, module="tx_life_watchdog_log_disk",
                trace=ctx.trace_id, tid=tid, seq=i,
                phase="ecriture concurrente du journal de debogage",
            )

    threads = [threading.Thread(target=_writer, args=(t,), name="tx_watch_%d" % t)
               for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ctx.data["a1_expected_lines"] = n_threads * n_lines

    # ===== A2 : aller-retour des valeurs speciales ======================
    for name, value in _SPECIAL_VALUES.items():
        flog_kv(
            "INFO", "TX_WATCH_KV", module="tx_life_watchdog_log_disk",
            trace=ctx.trace_id, case=name, payload=value,
        )
    ctx.data["a2_cases"] = dict(_SPECIAL_VALUES)

    # ===== A3 : message enorme ==========================================
    huge = "x" * (300 * 1024)
    flog_kv(
        "INFO", "TX_WATCH_HUGE", module="tx_life_watchdog_log_disk",
        trace=ctx.trace_id, size=len(huge), payload=huge,
    )
    ctx.data["a3_size"] = len(huge)

    # ===== A4 : CycleStats sous concurrence =============================
    stats = CycleStats(trace_id=ctx.trace_id)
    per_thread = 500

    def _bumper() -> None:
        for _ in range(per_thread):
            stats.add("apply_ok")
            stats.add("plan_actions", 2)

    bumpers = [threading.Thread(target=_bumper, name="tx_watch_stat_%d" % i)
               for i in range(n_threads)]
    for t in bumpers:
        t.start()
    for t in bumpers:
        t.join()
    snap = stats.snapshot()
    ctx.data["a4_apply_ok"] = snap.get("apply_ok")
    ctx.data["a4_plan_actions"] = snap.get("plan_actions")
    ctx.data["a4_expected_apply_ok"] = n_threads * per_thread
    ctx.data["a4_expected_plan_actions"] = n_threads * per_thread * 2

    finalize_results = []
    finalize_lock = threading.Lock()

    def _finalizer() -> None:
        res = stats.finalize("rewind", elapsed_ms=42)
        with finalize_lock:
            finalize_results.append(res)

    finalizers = [threading.Thread(target=_finalizer, name="tx_watch_fin_%d" % i)
                  for i in range(4)]
    for t in finalizers:
        t.start()
    for t in finalizers:
        t.join()
    ctx.data["a4_finalize_calls"] = len(finalize_results)

    # ===== A5 : observability sans echappement ==========================
    # Nom de cycle distinct de A4 : les deux lignes CYCLE_SUMMARY portent le
    # meme trace_id, il faut pouvoir les distinguer a la relecture.
    log_cycle_summary(
        ctx.trace_id, "event_restore",
        {
            "raw": 12,
            "plan_skipped": "3 evenements sans cle primaire",
            "apply_ok": 9,
        },
        elapsed_ms=17,
    )
    assert_invariant(
        False, "tx_watch_probe",
        trace=ctx.trace_id,
        detail="couche absente du projet",
        eid=77,
    )

    # ===== B : surveillance disque ======================================
    journal_path = ctx.data["journal_path"]
    original_shutil = dm.shutil
    try:
        # B1 : 50 Mo libres -> critique
        _FakeShutil.free = 50 * _MB
        dm.shutil = _FakeShutil
        st = dm.check_disk_for_path(journal_path)
        health = check_disk_space(journal_path)
        ctx.data["b1_status"] = tuple(st)
        ctx.data["b1_should_disable"] = health.should_disable_tracking
        ctx.data["b1_level"] = health.level
        ctx.data["b1_message"] = dm.format_disk_message(st)

        # B2 : l'espace revient
        _FakeShutil.free = 20 * _GB
        st = dm.check_disk_for_path(journal_path)
        health = check_disk_space(journal_path)
        ctx.data["b2_should_disable"] = health.should_disable_tracking
        ctx.data["b2_level"] = health.level
        ctx.data["b2_message"] = dm.format_disk_message(st)

        # B5 : zone intermediaire (300 Mo) -> "faible" mais pas critique
        _FakeShutil.free = 300 * _MB
        st = dm.check_disk_for_path(journal_path)
        health = check_disk_space(journal_path)
        ctx.data["b5_is_low"] = st.is_low
        ctx.data["b5_message"] = dm.format_disk_message(st)
        ctx.data["b5_should_disable"] = health.should_disable_tracking

        # B4 : le peripherique repond par une erreur
        dm.shutil = _RaisingShutil
        st = dm.check_disk_for_path(journal_path)
        health = check_disk_space(journal_path)
        ctx.data["b4_status"] = tuple(st)
        ctx.data["b4_level"] = health.level
        ctx.data["b4_should_disable"] = health.should_disable_tracking
    finally:
        dm.shutil = original_shutil

    # B3 : volume injoignable (pas de simulation, vrai chemin mort)
    dead = os.path.join("Z:" + os.sep, "partage_debranche", "recoverland_audit.sqlite")
    st = dm.check_disk_for_path(dead)
    health = check_disk_space(dead)
    ctx.data["b3_status"] = tuple(st)
    ctx.data["b3_level"] = health.level
    ctx.data["b3_should_disable"] = health.should_disable_tracking
    ctx.data["b3_message"] = dm.format_disk_message(st)

    flog(
        f"tx_life_watchdog_log_disk run end: trace_id={ctx.trace_id} "
        f"b1={ctx.data['b1_level']} b2={ctx.data['b2_level']} "
        f"b3={ctx.data['b3_level']} b4={ctx.data['b4_level']} "
        f"finalize_calls={ctx.data['a4_finalize_calls']}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains, assert_counter

    out = []
    d = ctx.data
    mine = [r for r in ctx.records if r.fields.get("trace") == ctx.trace_id]

    # ===== A1 : ecriture concurrente ====================================
    conc = [r for r in mine if r.fields.get("event") == "TX_WATCH_CONC"]
    out.append((
        "a1_concurrent_lines_all_written",
        len(conc) == d.get("a1_expected_lines"),
        f"lignes retrouvees={len(conc)} attendu={d.get('a1_expected_lines')} : "
        f"des lignes emises depuis 8 threads en parallele manquent dans "
        f"recoverland_debug.log, donc l'historique d'un incident est incomplet",
    ))
    scrambled = [
        r.raw[:110] for r in conc
        if r.fields.get("module") != "tx_life_watchdog_log_disk"
        or not (r.fields.get("tid") or "").isdigit()
        or not (r.fields.get("seq") or "").isdigit()
    ]
    out.append((
        "a1_concurrent_lines_not_interleaved",
        not scrambled,
        f"{len(scrambled)} ligne(s) melangees entre threads, ex: {scrambled[:2]} "
        f": deux threads ont ecrit dans la meme ligne physique et parse_log "
        f"reconstruit des paires cle=valeur fausses",
    ))

    # ===== A2 : aller-retour des valeurs speciales ======================
    kv = {r.fields.get("case"): r for r in mine
          if r.fields.get("event") == "TX_WATCH_KV"}
    broken = []
    for case, value in (d.get("a2_cases") or {}).items():
        rec = kv.get(case)
        if rec is None:
            broken.append((case, "ligne absente", value))
            continue
        got = rec.fields.get("payload")
        if got != value:
            broken.append((case, got, value))
    out.append((
        "a2_special_values_survive_the_round_trip",
        not broken,
        f"{len(broken)} valeur(s) ne reviennent pas intactes : "
        f"{[(c, repr(g)[:40], repr(w)[:40]) for c, g, w in broken]}. Une valeur "
        f"d'attribut multi-ligne (commentaire, adresse) ou un message "
        f"d'exception sur plusieurs lignes coupe l'enregistrement en deux : la "
        f"seconde moitie n'a plus d'horodatage et est jetee par parse_log, la "
        f"premiere se termine sur une quote ouverte. La ligne devient "
        f"inexploitable pour le diagnostic.",
    ))

    # ===== A3 : message enorme ==========================================
    huge = [r for r in mine if r.fields.get("event") == "TX_WATCH_HUGE"]
    huge_ok = bool(huge) and huge[0].fields.get("payload", "") \
        and len(huge[0].fields.get("payload", "")) == d.get("a3_size")
    out.append((
        "a3_huge_message_stays_on_one_line",
        bool(huge_ok),
        f"message de {d.get('a3_size')} octets : lignes retrouvees={len(huge)}, "
        f"taille relue={len(huge[0].fields.get('payload', '')) if huge else 0}. "
        f"Un gros payload (liste de FID d'une suppression massive, WKB en "
        f"base64) doit rester sur une ligne unique et relisible.",
    ))

    # ===== A4 : CycleStats ==============================================
    out.append((
        "a4_cycle_stats_counters_are_exact",
        d.get("a4_apply_ok") == d.get("a4_expected_apply_ok")
        and d.get("a4_plan_actions") == d.get("a4_expected_plan_actions"),
        f"apply_ok={d.get('a4_apply_ok')} attendu={d.get('a4_expected_apply_ok')} "
        f"plan_actions={d.get('a4_plan_actions')} attendu="
        f"{d.get('a4_expected_plan_actions')} : les compteurs d'un cycle de "
        f"REWIND sont incrementes depuis plusieurs phases ; un compteur faux "
        f"declenche de faux diagnostics d'anomalie",
    ))
    out.append(assert_counter(
        ctx.records,
        rf"\[{ctx.trace_id}\] CYCLE_SUMMARY cycle=rewind",
        1,
        name="a4_finalize_is_idempotent_under_concurrency",
    ))

    # ===== A5 : observability sans echappement ==========================
    summary = [r for r in ctx.records
               if f"[{ctx.trace_id}] CYCLE_SUMMARY cycle=event_restore" in r.raw]
    summary_fields = summary[0].fields if summary else {}
    out.append((
        "a5_cycle_summary_stays_parsable",
        summary_fields.get("plan_skipped") == "3 evenements sans cle primaire"
        and summary_fields.get("apply_ok") == "9",
        f"CYCLE_SUMMARY relu : plan_skipped="
        f"{summary_fields.get('plan_skipped')!r} apply_ok="
        f"{summary_fields.get('apply_ok')!r} elapsed_ms="
        f"{summary_fields.get('elapsed_ms')!r}. log_cycle_summary concatene "
        f"`{{key}}={{value}}` sans passer par _format_kv_value : des qu'une "
        f"valeur contient une espace, toutes les paires suivantes sont "
        f"decalees et la ligne de synthese d'un REWIND — celle que l'on relit "
        f"en premier apres un incident — devient fausse.",
    ))
    violation = [r for r in ctx.records
                 if "INVARIANT_VIOLATED name=tx_watch_probe" in r.raw]
    violation_fields = violation[0].fields if violation else {}
    out.append((
        "a5_invariant_violation_stays_parsable",
        violation_fields.get("detail") == "couche absente du projet"
        and violation_fields.get("eid") == "77",
        f"INVARIANT_VIOLATED relu : detail="
        f"{violation_fields.get('detail')!r} eid={violation_fields.get('eid')!r} "
        f"— meme defaut d'echappement dans assert_invariant, sur la ligne "
        f"CRITICAL censee etre la plus exploitable du journal",
    ))

    # ===== B1 : garantie acquise ========================================
    out.append((
        "b1_critical_disk_disables_tracking",
        d.get("b1_should_disable") is True and d.get("b1_level") == "critical"
        and "critique" in (d.get("b1_message") or ""),
        f"50 Mo libres -> level={d.get('b1_level')} "
        f"should_disable_tracking={d.get('b1_should_disable')} "
        f"message={d.get('b1_message')!r} : le seuil critique doit couper "
        f"l'enregistrement avant que SQLite ne rate une ecriture",
    ))

    # ===== B2 : et quand l'espace revient ? =============================
    out.append((
        "b2_recovered_disk_is_healthy_again",
        d.get("b2_should_disable") is False and d.get("b2_level") == "healthy",
        f"20 Go libres -> level={d.get('b2_level')} "
        f"should_disable_tracking={d.get('b2_should_disable')} : le calcul pur "
        f"redevient bien sain",
    ))

    # ===== B3 : volume injoignable ======================================
    out.append((
        "b3_unreachable_volume_is_not_reported_healthy",
        d.get("b3_level") != "healthy",
        f"volume injoignable -> DiskStatus={d.get('b3_status')} "
        f"level={d.get('b3_level')} should_disable={d.get('b3_should_disable')} "
        f"message={d.get('b3_message')!r}. Zero octet libre est classe « sain » : "
        f"un journal pose sur un partage reseau debranche ne declenchera jamais "
        f"la surveillance, et chaque ecriture echouera en silence pendant que la "
        f"barre d'etat affiche un journal en bonne sante.",
    ))

    # ===== B4 : branche OSError =========================================
    out.append((
        "b4_unreadable_volume_is_treated_as_critical",
        d.get("b4_should_disable") is True,
        f"peripherique non pret -> DiskStatus={d.get('b4_status')} "
        f"level={d.get('b4_level')} should_disable={d.get('b4_should_disable')}. "
        f"La branche OSError rend is_low=True mais is_critical=False avec 0 "
        f"octet libre : 0 est pourtant sous le seuil critique de 100 Mo. Le "
        f"suivi reste actif alors que le volume ne repond plus.",
    ))

    # ===== B5 : le seuil "faible" sert-il ? =============================
    out.append((
        "b5_low_threshold_is_computed",
        d.get("b5_is_low") is True and "faible" in (d.get("b5_message") or ""),
        f"300 Mo libres -> is_low={d.get('b5_is_low')} "
        f"message={d.get('b5_message')!r} : le calcul du seuil intermediaire "
        f"est correct",
    ))

    # ===== Gardes de source =============================================
    recover_src = (_PLUGIN_ROOT / "recover.py").read_text(
        encoding="utf-8", errors="replace")
    monitor = recover_src[
        recover_src.find("def _start_disk_monitor("):
        recover_src.find("def _stop_disk_monitor(")
    ]
    out.append((
        "b5_low_threshold_is_surfaced_to_the_user",
        "is_low" in monitor,
        "_start_disk_monitor._check ne regarde que status.is_critical : le "
        "message « Espace disque faible » construit par format_disk_message "
        "n'est emis nulle part dans le plugin. L'utilisateur n'a aucun "
        "avertissement precoce, seulement la coupure brutale a 100 Mo.",
    ))
    out.append((
        "b2_tracking_is_re_enabled_when_space_returns",
        ".activate()" in monitor or "tracking_enabled\", True" in monitor,
        "_check desactive le suivi et ecrit RecoverLand/tracking_enabled=False "
        "dans QgsSettings, mais aucune branche ne le rallume quand l'espace "
        "revient — et le reglage est persistant. L'utilisateur libere son "
        "disque, continue a travailler, et RecoverLand n'enregistre plus rien "
        "jusqu'a ce qu'il pense a recliquer sur l'indicateur de la barre "
        "d'etat.",
    ))

    obs_src = (_PLUGIN_ROOT / "core" / "observability.py").read_text(
        encoding="utf-8", errors="replace")
    out.append((
        "observability_reuses_the_kv_formatter",
        "_format_kv_value" in obs_src or "flog_kv" in obs_src,
        "core/observability.py construit ses lignes cle=valeur a la main alors "
        "que core/logger.py expose deja _format_kv_value / flog_kv ; les deux "
        "formats divergent et seul l'un des deux est relisible par "
        "scripts/validation/parse_log.py",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"tx_life_watchdog_log_disk.*trace_id={ctx.trace_id}",
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
