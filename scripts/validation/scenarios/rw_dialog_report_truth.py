"""Scenario RLU-055/RLU-056: the dialog must display what the run did.

Two lies were measured on the owner's real runs, both of them in the
message the user reads at the end of a cycle. This scenario locks both,
on the real `recoverland.recover_dialog` module.

A. The end-of-run message contradicted the log line of the same run.
   The plan carried 73 actions and 6 entities came back:

       CYCLE_SUMMARY ... plan_actions=73 apply_ok=6 apply_skipped=3
                         apply_fail=64 failed_target_absent=59
                         failed_geometry_drift=5 skipped_idempotent=3
       dialog        ... "14 restauree(s), 59 echouee(s)."

   `total_ok` counts everything the provider did not reject (6 writes + 3
   idempotent skips + 5 drift refusals = 14) and `total_fail` counts only
   the actions that reached the writer, so 5 failures were hidden. The
   message must come from the SAME buckets as the log line, must total
   the plan, and must never count a skip as a failure.

B. The plugin measured its own failure and announced a success. In the
   same cycle:

       CYCLE_SUMMARY cycle=undo apply_ok=273 apply_fail=0
       REWIND_STATE [after_undo] zone_mkt_rip expected_delta=+5
                                 actual_delta=+9 [MISMATCH] missing=-4
       REWIND_STATE [after_undo] za_nro       expected_delta=-4
                                 actual_delta=+1 [MISMATCH] missing=-5

   `_rewind_count_snapshot` counted the entities before and after, found
   the gap, printed it in the log file and dropped it. The user was told
   the cycle succeeded while two layers held a state he never asked for.
   A mismatch means the layer is NOT at the requested state, whatever the
   number of actions that reported success.

Everything here runs offline on the real functions: the counting path is
driven with fake layers through the module-level `find_target_layer`, and
the formatting/reporting path is called on a stub carrying only what those
methods actually use (`tr`, `_state_mismatches`, `_get_dialog_read_conn`).
The wiring is locked by source: a report that is never displayed is the
failure mode this plugin has already paid for four times.
"""
import inspect
import sys
from pathlib import Path

SCENARIO_ID = "rw_dialog_report_truth"
INVARIANT = "I-LOG"

# Log signature of the new user-facing report:
EXPECTED_SIGNATURE = r"STATE_MISMATCH layer=.*missing=[-+]\d+"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PLUGINS_PARENT = _PLUGIN_ROOT.parent
if str(_PLUGINS_PARENT) not in sys.path:
    sys.path.insert(0, str(_PLUGINS_PARENT))


# --------------------------------------------------------------------------
# Fakes: the smallest objects the real code under test actually touches.
# --------------------------------------------------------------------------
class FakeEvent:
    """What `_rewind_count_snapshot` reads off an audit event."""

    def __init__(self, layer_name: str, operation_type: str, eid: int = 0):
        self.layer_name_snapshot = layer_name
        self.operation_type = operation_type
        self.event_id = eid
        self.feature_identity_json = "{}"


class FakeLayer:
    def __init__(self, count: int):
        self._count = count

    def featureCount(self):
        return self._count


class DialogStub:
    """Vehicle for the REAL dialog methods, carrying only what they use.

    Deliberately NOT a QDialog: building the real dialog would drag the
    whole UI into the scenario and hide which attributes the reporting
    path truly depends on. `setup` grafts the real, unmodified methods of
    `RecoverDialog` onto this class, so what runs below is the shipped
    code and not a paraphrase of it.
    """

    # Grafted in setup() from RecoverDialog:
    _rewind_count_snapshot = None
    _format_state_mismatches = None
    _report_state_mismatches = None
    _format_restore_outcome = None
    _on_restore_runner_layer = None

    def __init__(self):
        self._state_mismatches = []
        self.phases = []

    def tr(self, text):
        return text

    def _get_dialog_read_conn(self):
        return None

    def update_phase(self, text):
        self.phases.append(text)


_GRAFTED = (
    "_rewind_count_snapshot",
    "_format_state_mismatches",
    "_report_state_mismatches",
    "_format_restore_outcome",
    "_on_restore_runner_layer",
)


def _mismatch_case():
    """The owner's two mismatching layers, rebuilt to the digit.

    undo semantics: expected_delta = n_INSERT - n_DELETE.
      zone_mkt_rip: 5 INSERT  -> expected +5; 100 -> 109 measured +9,
                    missing = +5 - (+9) = -4  (4 entities too many)
      za_nro:       4 DELETE  -> expected -4; 200 -> 201 measured +1,
                    missing = -4 - (+1) = -5  (5 entities too many)
      voirie:       2 INSERT  -> expected +2;  50 ->  52 measured +2, OK
      reseau:       3 INSERT  -> expected +3; layer unresolvable, count
                    unknown -> no verdict, and above all no invented one.
    """
    by_ds = {
        "fp_zone": [FakeEvent("zone_mkt_rip", "INSERT", i) for i in range(5)],
        "fp_za": [FakeEvent("za_nro", "DELETE", 10 + i) for i in range(4)],
        "fp_voirie": [FakeEvent("voirie", "INSERT", 20 + i) for i in range(2)],
        "fp_reseau": [FakeEvent("reseau", "INSERT", 30 + i) for i in range(3)],
    }
    before = {"fp_zone": 100, "fp_za": 200, "fp_voirie": 50, "fp_reseau": -1}
    after = {"fp_zone": 109, "fp_za": 201, "fp_voirie": 52, "fp_reseau": None}
    return by_ds, before, after


# The real 2026-08-12 rewind, as the runner classified it.
_REAL_BREAKDOWN = {
    "applied": 6,
    "applied_partial": 0,
    "skipped_idempotent": 3,
    "failed": 0,
    "failed_target_absent": 59,
    "failed_geometry_drift": 5,
}
_REAL_PLAN_ACTIONS = 73


def setup(ctx):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    from recoverland.core.workflow_service import GroupedRestoreResult

    missing = [name for name in _GRAFTED
               if not callable(getattr(rd.RecoverDialog, name, None))]
    if missing:
        raise AssertionError(
            "RecoverDialog n'expose pas " + ", ".join(missing)
            + " : sans ces methodes le constat mesure ne peut pas etre "
              "remonte a l'utilisateur.")
    for name in _GRAFTED:
        setattr(DialogStub, name, getattr(rd.RecoverDialog, name))

    ctx.data["rd"] = rd
    ctx.data["rr"] = rr
    ctx.data["GroupedRestoreResult"] = GroupedRestoreResult
    ctx.data["rd_source"] = (
        _PLUGIN_ROOT / "recover_dialog.py").read_text(encoding="utf-8")
    ctx.data["rr_source"] = (
        _PLUGIN_ROOT / "restore_runner.py").read_text(encoding="utf-8")


def run(ctx):
    rd = ctx.data["rd"]
    rr = ctx.data["rr"]
    GroupedRestoreResult = ctx.data["GroupedRestoreResult"]
    from recoverland.core.logger import flog

    flog(f"scenario_run trace_id={ctx.trace_id} scenario={ctx.scenario_id}")

    # ---------------------------------------------------------------- A
    # The runner report, then the buckets, then the message. Three real
    # functions chained exactly as the finished slot chains them.
    report = rr.build_apply_report(_REAL_BREAKDOWN, _REAL_PLAN_ACTIONS)
    ctx.data["report"] = report

    # The result the runner ALSO emits, carrying the two totals that lied.
    result = GroupedRestoreResult(
        total_ok=14, total_fail=59, errors=[], by_ds={}, trace_events=[],
        applied=6, skipped_idempotent=3, failed=0,
        failed_target_absent=59, failed_geometry_drift=5,
    )
    buckets = rd._restore_buckets(result, partial_n=0, report=report)
    ctx.data["buckets"] = buckets
    stub = DialogStub()
    ctx.data["msg"] = stub._format_restore_outcome(buckets)

    # Antithesis 1: no report available (runner already released). The
    # buckets must then be rebuilt from the result WITHOUT passing their
    # own sum off as the plan.
    ctx.data["buckets_no_report"] = rd._restore_buckets(result, report=None)
    ctx.data["msg_no_report"] = stub._format_restore_outcome(
        ctx.data["buckets_no_report"])

    # Antithesis 2: a run that is nothing but skips must announce zero
    # failure -- a skip is the target state, not an incident.
    skip_only = rr.build_apply_report(
        {"applied": 0, "skipped_idempotent": 4}, 4)
    ctx.data["buckets_skip"] = rd._restore_buckets(
        GroupedRestoreResult(total_ok=4, total_fail=0, errors=[], by_ds={},
                             trace_events=[], skipped_idempotent=4),
        report=skip_only)
    ctx.data["msg_skip"] = stub._format_restore_outcome(
        ctx.data["buckets_skip"])

    # ---------------------------------------------------------------- B
    # Drive the REAL counting path with fake layers.
    by_ds, before, after = _mismatch_case()
    fp_of = {id(evts[0]): fp for fp, evts in by_ds.items()}

    def fake_find_target_layer(event, read_conn=None):
        for fp, evts in by_ds.items():
            if evts and evts[0] is event:
                count = after[fp]
                return None if count is None else FakeLayer(count)
        return None

    original_find = rd.find_target_layer
    original_qlog = rd.qlog
    captured = []

    def fake_qlog(message, level="INFO"):
        captured.append((message, level))

    rd.find_target_layer = fake_find_target_layer
    rd.qlog = fake_qlog
    try:
        snap = DialogStub()
        ctx.data["counts"] = snap._rewind_count_snapshot(
            "after_undo", by_ds, before=before)
        ctx.data["mismatches"] = list(snap._state_mismatches)
        ctx.data["reported"] = snap._report_state_mismatches()
        ctx.data["report_msg"] = captured[-1] if captured else ("", "")
        # Drained: the same measurement must never be announced twice.
        n_before_second = len(captured)
        ctx.data["reported_twice"] = snap._report_state_mismatches()
        ctx.data["second_call_silent"] = (len(captured) == n_before_second)

        # Antithesis: a cycle where every layer landed on its target must
        # stay silent -- a warning that cries wolf ends up ignored.
        ok_snap = DialogStub()
        ok_snap._rewind_count_snapshot(
            "after_undo", {"fp_voirie": by_ds["fp_voirie"]},
            before={"fp_voirie": 50})
        ctx.data["ok_mismatches"] = list(ok_snap._state_mismatches)
        n_before_ok = len(captured)
        ctx.data["ok_reported"] = ok_snap._report_state_mismatches()
        ctx.data["ok_silent"] = (len(captured) == n_before_ok)

        # A `before_*` snapshot has nothing to compare: it must not touch
        # the pending list of the transition that is about to be judged.
        pre_snap = DialogStub()
        pre_snap._state_mismatches = [{"sentinel": True}]
        pre_snap._rewind_count_snapshot("before_undo", by_ds, before=None)
        ctx.data["pre_untouched"] = pre_snap._state_mismatches
    finally:
        rd.find_target_layer = original_find
        rd.qlog = original_qlog
    del fp_of

    # ---------------------------------------------------------------- C
    # The cycles measured on the graphics thread ran 95 s, 283 s, and one
    # died in flight at 282 s. The work still runs there; what must be
    # guaranteed is that the wait is legible. The runner announces the
    # layer it is about to write, and the announcement really leaves the
    # runner: `UndoRunner._process_next_group` is called for real, with a
    # resolver that finds nothing, so the emission is observed and not
    # merely read in the source.
    undo_events = [FakeEvent("za_nro", "DELETE", 40 + i) for i in range(7)]
    runner = rr.UndoRunner({"fp_za": undo_events}, lambda evt: None)
    seen = []
    runner.layer_started.connect(
        lambda name, idx, total, n: seen.append((name, idx, total, n)))
    runner._process_next_group()
    ctx.data["emitted"] = list(seen)

    phase_stub = DialogStub()
    if seen:
        phase_stub._on_restore_runner_layer(*seen[0])
    ctx.data["phases"] = list(phase_stub.phases)


def assertions(ctx):
    rd_src = ctx.data["rd_source"]
    report = ctx.data["report"]
    buckets = ctx.data["buckets"]
    msg = ctx.data["msg"]
    mismatches = ctx.data["mismatches"]
    report_msg, report_level = ctx.data["report_msg"]

    # Bodies of the two dialog slots and of the deferred feedback.
    def body(name, src=rd_src):
        after = src.split(f"def {name}(", 1)
        if len(after) == 1:
            return ""
        tail = after[1]
        # Stop at the next top-level method definition.
        cut = tail.find("\n    def ")
        return tail[:cut] if cut != -1 else tail

    def code_only(text):
        """Drop docstrings and whole-line comments.

        A prose reference to `total_ok` in the docstring that EXPLAINS why
        the counter must not be read is not a read. Without this the
        source lock would forbid documenting the defect it closes.
        """
        out = []
        in_doc = False
        for line in text.split("\n"):
            stripped = line.strip()
            if in_doc:
                if '"""' in stripped:
                    in_doc = False
                continue
            if stripped.startswith('"""'):
                if stripped.count('"""') == 1:
                    in_doc = True
                continue
            if stripped.startswith("#"):
                continue
            out.append(line)
        return "\n".join(out)

    outcome_params = list(
        inspect.signature(ctx.data["rd"].RecoverDialog._format_restore_outcome)
        .parameters)

    version_body = body("_on_version_restore_done")
    event_body = body("_on_event_restore_done")
    feedback_body = body("_display_restore_feedback")
    snapshot_body = body("_rewind_count_snapshot")
    outcome_body = body("_format_restore_outcome")
    undo_rewind_body = body("_on_auto_undo_then_rewind")

    by_name = {m["layer"]: m for m in mismatches}
    zone = by_name.get("zone_mkt_rip", {})
    za = by_name.get("za_nro", {})

    emitted = ctx.data["emitted"]
    phases = ctx.data["phases"]
    # Every creation of a runner that can hold the thread for minutes.
    n_long_runners = (rd_src.count("StrictRestoreRunner(")
                      + rd_src.count("UndoRunner("))

    return [
        # ---------------------------------------------------------- A
        ("A_report_buckets_total_the_plan",
         report["apply_ok"] + report["apply_skipped"] + report["apply_fail"]
         + report["cancelled"] == _REAL_PLAN_ACTIONS
         and report["plan_actions"] == _REAL_PLAN_ACTIONS,
         f"apply_ok={report['apply_ok']} + apply_skipped="
         f"{report['apply_skipped']} + apply_fail={report['apply_fail']} "
         f"+ cancelled={report['cancelled']} doivent totaliser les "
         f"{_REAL_PLAN_ACTIONS} actions du plan (plan_actions="
         f"{report['plan_actions']}). Un compte rendu qui ne totalise pas "
         "le plan ne dit pas combien d'entites sont revenues."),
        ("A_displayed_buckets_total_the_plan",
         (buckets["applied"] + buckets["applied_partial"]
          + buckets["skipped"] + buckets["failed_total"]
          + buckets["cancelled"]) == buckets["planned"]
         and buckets["planned"] == _REAL_PLAN_ACTIONS,
         f"seaux affiches: applied={buckets['applied']} "
         f"partial={buckets['applied_partial']} skipped={buckets['skipped']} "
         f"failed={buckets['failed_total']} cancelled={buckets['cancelled']} "
         f"pour planned={buckets['planned']} : le message doit totaliser le "
         "plan comme la ligne de log."),
        ("A_message_names_the_six_entities_that_came_back",
         "6 restauree(s)" in msg and "14" not in msg,
         f"message={msg!r} : 6 entites sont revenues. La valeur 14 est "
         "total_ok, qui compte aussi les 3 sauts et les 5 refus de derive."),
        ("A_message_publishes_the_failure_total",
         "64 echouee(s)" in msg,
         f"message={msg!r} : 64 actions ont echoue (59 sans cible + 5 "
         "derives). Afficher 59 (total_fail) cache cinq echecs."),
        ("A_message_names_the_failure_reasons",
         "59 sans cible" in msg and "5 refusee(s)" in msg,
         f"message={msg!r} : sans le motif, l'utilisateur ne sait pas s'il "
         "doit chercher une couche disparue ou une geometrie modifiee."),
        ("A_message_states_the_plan_size",
         "73 action(s) planifiee(s)" in msg,
         f"message={msg!r} : la taille du plan est la base contre laquelle "
         "l'utilisateur additionne les seaux."),
        ("A_skip_is_not_a_failure",
         buckets["skipped"] == 3 and "3 deja a l'etat cible" in msg
         and buckets["failed_total"] == 64,
         f"skipped={buckets['skipped']} failed_total={buckets['failed_total']} "
         f"message={msg!r} : un saut (insert_target_gone, "
         "skipped_idempotent) est l'etat demande, pas un echec."),
        ("A_neg_skip_only_run_announces_no_failure",
         ctx.data["buckets_skip"]["failed_total"] == 0
         and "echouee" not in ctx.data["msg_skip"],
         f"message={ctx.data['msg_skip']!r} : 4 sauts et zero echec ; "
         "annoncer un echec ici enverrait chercher une panne inexistante."),
        ("A_neg_no_report_does_not_fake_a_plan",
         ctx.data["buckets_no_report"]["plan_known"] is False
         and "planifiee" not in ctx.data["msg_no_report"]
         and "comptabilisee" in ctx.data["msg_no_report"],
         f"message={ctx.data['msg_no_report']!r} : sans le report du runner "
         "la somme des seaux n'est pas la taille du plan et ne doit pas "
         "etre presentee comme telle."),
        ("A_outcome_formatter_ignores_the_two_totals",
         outcome_params == ["self", "buckets"]
         and "total_ok" not in code_only(outcome_body)
         and "total_fail" not in code_only(outcome_body),
         f"signature={outcome_params} attendu ['self', 'buckets'] : "
         "_format_restore_outcome ne recoit que les seaux, donc il ne peut "
         "structurellement pas repartir de total_ok/total_fail. Lui passer "
         "le resultat rouvrirait le defaut."),
        ("A_old_lying_message_is_gone",
         "{ok} restauree(s), {fail} echouee(s)" not in rd_src,
         "la chaine '{ok} restauree(s), {fail} echouee(s)' etait alimentee "
         "par total_ok/total_fail ; elle a annonce 14 restaurees pour 6 "
         "entites revenues."),
        ("A_rewind_slot_reports_through_the_buckets",
         "_restore_buckets(" in version_body
         and "_format_restore_outcome(" in version_body
         and "_runner_apply_report(" in version_body,
         "_on_version_restore_done doit demander son report au runner puis "
         "formater les seaux ; sinon le message et la ligne CYCLE_SUMMARY "
         "du meme run racontent deux histoires."),
        ("A_event_restore_slot_reports_through_the_buckets",
         "_restore_buckets(" in event_body
         and "_runner_apply_report(" in event_body
         and "_format_restore_outcome(" in feedback_body,
         "le restore d'un evenement passe par un feedback differe : les "
         "seaux doivent y arriver aussi, sans quoi ce chemin garde le "
         "message que l'on vient de retirer de l'autre."),

        # ---------------------------------------------------------- B
        ("B_mismatch_is_detected_on_both_layers",
         len(mismatches) == 2 and "zone_mkt_rip" in by_name
         and "za_nro" in by_name,
         f"couches en ecart relevees={sorted(by_name)} attendu "
         "['za_nro', 'zone_mkt_rip'] : ce sont les deux couches du journal "
         "du proprietaire, mesurees a partir des memes comptes."),
        ("B_zone_mkt_rip_numbers_match_the_journal",
         zone.get("expected_delta") == 5 and zone.get("actual_delta") == 9
         and zone.get("missing") == -4,
         f"zone_mkt_rip expected_delta={zone.get('expected_delta')} "
         f"actual_delta={zone.get('actual_delta')} "
         f"missing={zone.get('missing')} attendus +5 / +9 / -4, soit la "
         "ligne REWIND_STATE relevee chez le proprietaire."),
        ("B_za_nro_numbers_match_the_journal",
         za.get("expected_delta") == -4 and za.get("actual_delta") == 1
         and za.get("missing") == -5,
         f"za_nro expected_delta={za.get('expected_delta')} "
         f"actual_delta={za.get('actual_delta')} missing={za.get('missing')} "
         "attendus -4 / +1 / -5."),
        ("B_mismatch_is_reported_to_the_user",
         ctx.data["reported"] is True and report_level == "WARNING"
         and "zone_mkt_rip" in report_msg and "za_nro" in report_msg,
         f"reported={ctx.data['reported']} level={report_level} "
         f"message={report_msg!r} : le produit comptait deja les entites "
         "avant et apres et detectait l'ecart, puis n'en faisait rien. "
         "C'est l'information la plus importante apres un rewind."),
        ("B_report_says_the_layer_is_not_in_the_requested_state",
         "ne sont pas dans l'etat demande" in report_msg,
         f"message={report_msg!r} : un MISMATCH signifie que la couche "
         "n'est pas dans l'etat demande, quel que soit le nombre d'actions "
         "reussies."),
        ("B_report_names_the_direction_of_the_gap",
         "4 entite(s) en trop" in report_msg
         and "5 entite(s) en trop" in report_msg
         and "manquante" not in report_msg,
         f"message={report_msg!r} : missing=-4 veut dire quatre entites EN "
         "TROP. Annoncer des entites manquantes envoie l'utilisateur "
         "chercher a l'oppose de son probleme."),
        ("B_report_gives_the_measured_counts",
         "100 -> 109" in report_msg and "200 -> 201" in report_msg,
         f"message={report_msg!r} : les comptes avant/apres sont ce que "
         "l'utilisateur peut verifier lui-meme dans la table."),
        ("B_neg_layer_at_target_is_not_reported",
         "voirie" not in report_msg and ctx.data["ok_mismatches"] == []
         and ctx.data["ok_reported"] is False and ctx.data["ok_silent"],
         f"ok_mismatches={ctx.data['ok_mismatches']} "
         f"reported={ctx.data['ok_reported']} : une couche arrivee a "
         "l'etat demande ne doit rien declencher, sinon l'avertissement "
         "finit ignore le jour ou il a raison."),
        ("B_neg_unknown_count_invents_no_mismatch",
         "reseau" not in by_name
         and ctx.data["counts"].get("fp_reseau") == -1,
         f"couches relevees={sorted(by_name)} : une couche non resolue "
         "compte -1 ; la comparer produirait un ecart tire d'une mesure "
         "absente, et un avertissement invérifiable est pire que le "
         "silence."),
        ("B_neg_report_is_drained",
         ctx.data["reported_twice"] is False
         and ctx.data["second_call_silent"],
         f"reported_twice={ctx.data['reported_twice']} : la meme mesure "
         "annoncee deux fois ferait douter des deux."),
        ("B_neg_before_snapshot_does_not_clear_pending",
         ctx.data["pre_untouched"] == [{"sentinel": True}],
         f"pending={ctx.data['pre_untouched']} : un releve 'before' n'a "
         "rien a comparer et ne doit pas effacer le constat de la "
         "transition qui va etre jugee."),
        ("B_snapshot_records_what_it_prints",
         "mismatches.append(" in snapshot_body
         and "_state_mismatches = mismatches" in snapshot_body,
         "_rewind_count_snapshot imprimait [MISMATCH] dans le journal et "
         "jetait le constat ; il doit le conserver pour que quelqu'un "
         "puisse le dire a l'utilisateur."),
        ("B_every_after_snapshot_is_followed_by_a_report",
         all("_report_state_mismatches()" in b
             for b in (version_body, undo_rewind_body)),
         "les deux transitions qui comparent des comptes "
         "(_on_version_restore_done, _on_auto_undo_then_rewind) doivent "
         "appeler _report_state_mismatches() : un constat mesure et jamais "
         "remonte est exactement le defaut que l'on ferme ici."),
        ("B_mismatch_downgrades_the_success_message",
         "state_mismatch = bool(self._state_mismatches)" in version_body
         and "and not state_mismatch" in version_body,
         "un rewind sans echec d'action mais dont une couche n'est pas a "
         "l'etat demande ne doit pas etre journalise comme un succes."),
        ("B_mismatch_is_greppable_in_the_log",
         "STATE_MISMATCH" in snapshot_body or "STATE_MISMATCH" in rd_src,
         "le constat doit aussi laisser une ligne dediee dans le journal : "
         "REWIND_STATE est un bloc multiligne que personne ne grep."),

        # ---------------------------------------------------------- C
        ("C_runner_announces_the_layer_it_opens",
         emitted == [("za_nro", 1, 1, 7)],
         f"emissions observees={emitted} attendu [('za_nro', 1, 1, 7)] : "
         "l'annonce doit partir du runner AVANT le travail qui tient le "
         "thread graphique, sinon l'etiquette nomme ce qui vient de finir."),
        ("C_phase_label_names_layer_and_position",
         len(phases) == 1 and "za_nro" in phases[0]
         and "1/1" in phases[0] and "7 evenement(s)" in phases[0],
         f"etiquette={phases!r} : sur un cycle de 283 s, savoir quelle "
         "couche est en cours et combien il en reste est la seule "
         "progression que le plugin puisse honnetement promettre."),
        ("C_both_long_runners_expose_the_signal",
         all(hasattr(getattr(ctx.data["rr"], cls), "layer_started")
             for cls in ("StrictRestoreRunner", "UndoRunner")),
         "les deux runners qui ecrivent pendant des minutes (rewind et "
         "undo) doivent tous les deux annoncer leurs couches."),
        ("C_every_long_runner_creation_connects_the_signal",
         rd_src.count("runner.layer_started.connect(") == n_long_runners,
         f"{rd_src.count('runner.layer_started.connect(')} connexion(s) "
         f"pour {n_long_runners} creation(s) de StrictRestoreRunner/"
         "UndoRunner : un signal qu'un seul chemin oublie de brancher est "
         "un remede ecrit et jamais branche, et ce plugin en a deja paye "
         "quatre."),
        ("C_cycle_end_stops_claiming_work_in_progress",
         "progress_phase_label.setVisible(False)" in body("_end_progress_ui")
         and all("_end_progress_ui()" in b
                 for b in (version_body, feedback_body,
                           body("_on_undo_done"),
                           body("_on_undo_session_done"))),
         "chaque fin de cycle doit eteindre l'etiquette de phase : la "
         "laisser sur 'Couche 12/12' apres le compte rendu annonce une "
         "ecriture encore en cours sur un cycle termine."),
    ]


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
