"""Restore preview formatter for RecoverLand (BL-06).

Formats RestorePlan and PreflightReport into human-readable
summaries for UI display. Pure logic, no QGIS dependency.
"""
from qgis.PyQt.QtCore import QCoreApplication

from .restore_contracts import (
    RestorePlan, PreflightReport, PreflightVerdict,
    RestoreMode, AtomicityPolicy,
)


def _tr(msg):
    return QCoreApplication.translate("RestorePreview", msg)


def format_plan_summary(plan: RestorePlan) -> str:
    """Format a restore plan as a human-readable summary."""
    mode_label = _tr("Evenement") if plan.mode == RestoreMode.EVENT else _tr("Temporel")
    lines = [
        _tr("Mode: {mode}").format(mode=mode_label),
        _tr("Entites: {count}").format(count=plan.entity_count),
        _tr("Evenements: {count}").format(count=plan.event_count),
    ]

    if plan.cutoff is not None:
        lines.append(f"Cutoff: {plan.cutoff.cutoff_type.value} = {plan.cutoff.value}")

    op_counts = _count_operations(plan)
    if op_counts:
        parts = [f"{count} {op}" for op, count in sorted(op_counts.items())]
        lines.append(_tr("Operations: {ops}").format(ops=", ".join(parts)))

    if plan.atomicity == AtomicityPolicy.STRICT:
        lines.append(_tr("Atomicite: tout-ou-rien (rollback si echec)"))
    else:
        lines.append(_tr("Atomicite: par entite (isolation des erreurs)"))

    return "\n".join(lines)


def format_preflight_report(report: PreflightReport) -> str:
    """Format a preflight report for UI display."""
    lines = [format_plan_summary(report.plan)]
    lines.append("")

    if report.verdict == PreflightVerdict.GO:
        lines.append(_tr("Statut: PRET"))
    elif report.verdict == PreflightVerdict.GO_WITH_WARNINGS:
        lines.append(_tr("Statut: PRET (avec avertissements)"))
        for w in report.warnings[:5]:
            lines.append(f"  - {w}")
    else:
        lines.append(_tr("Statut: BLOQUE"))
        for b in report.blocking_reasons[:5]:
            lines.append(f"  * {b}")
        if report.warnings:
            lines.append(_tr("Avertissements:"))
            for w in report.warnings[:3]:
                lines.append(f"  - {w}")

    return "\n".join(lines)


def format_dry_run_message(report: PreflightReport) -> str:
    """Format a dry-run result message for confirmation dialog."""
    plan = report.plan
    mode_label = _tr("evenement") if plan.mode == RestoreMode.EVENT else _tr("temporel")

    if report.verdict == PreflightVerdict.BLOCKED:
        reasons = "; ".join(report.blocking_reasons[:3])
        return _tr("Restauration bloquee: {reasons}").format(reasons=reasons)

    msg = _tr(
        "Restauration {mode}: "
        "{event_count} evenement(s) sur {entity_count} entite(s)"
    ).format(
        mode=mode_label,
        event_count=plan.event_count,
        entity_count=plan.entity_count,
    )

    if report.warnings:
        msg += _tr("\n\nAvertissements ({count}):").format(count=len(report.warnings))
        for w in report.warnings[:3]:
            msg += f"\n  - {w}"

    msg += "\n\n" + _tr("Continuer ?")
    return msg


def _count_operations(plan: RestorePlan) -> dict:
    """Count compensatory operations in a plan."""
    counts: dict = {}
    for action in plan.actions:
        op = action.compensatory_op
        counts[op] = counts.get(op, 0) + 1
    return counts
