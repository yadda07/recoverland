"""tx_journal_pending_hostile - RecoverLand validation runtime.

Invariant: I-JRN / zero perte silencieuse (le filet de securite ne doit
jamais devenir la cause de la perte).

Le mecanisme
------------
Quand la WriteQueue ne peut pas ecrire (file saturee, arret brutal, erreur
SQLite), `integrity.save_pending_events` depose les evenements dans
`recoverland_pending.json`, a cote du journal. Au demarrage suivant,
`check_journal_integrity` -> `_recover_pending_events` doit les remettre
dans la base.

Ce fichier est ecrit par un processus qui est justement en train de mourir.
Il est donc, par construction, le fichier le plus susceptible d'etre
tronque, vide, ou a moitie ecrit. `_recover_pending_events` prend trois
decisions irreversibles :

    if not isinstance(events, list) or len(events) == 0:
        os.remove(pending_path)          # (1) suppression sans lecture
        return 0, 0
    ...
    if remaining: _rewrite_pending_events(...)   # (2) conservation
    os.remove(pending_path)                      # (3) suppression apres succes

La branche (1) est la dangereuse : elle supprime le fichier des que le JSON
racine n'est pas une liste. Un JSON valide mais de forme differente - par
exemple un objet `{"events": [...]}` produit par une version anterieure,
une fusion de fichiers, ou une reprise manuelle - est detruit sans qu'une
seule ligne soit lue, sans journal d'erreur, et sans que l'utilisateur soit
prevenu. Le message affiche au demarrage reste "journal sain".

Le scenario d'echec concret
---------------------------
QGIS meurt pendant une saisie de terrain. 40 evenements partent dans le
fichier d'attente. Au redemarrage, si ce fichier n'a pas EXACTEMENT la forme
attendue, il est efface : les 40 modifications ne sont plus nulle part - ni
dans la base, ni dans le fichier d'attente. L'utilisateur croit son
historique complet ; le trou n'apparaitra qu'au premier REWIND qui traverse
cette periode, des mois plus tard.

Ce que le scenario execute reellement
-------------------------------------
Six fichiers d'attente hostiles, chacun a cote d'un vrai journal SQLite dans
son propre dossier temporaire, passes au VRAI
`check_journal_integrity` :

    ordures       octets qui ne sont pas du JSON
    vide          fichier de zero octet (ecriture interrompue)
    liste_vide    "[]" - rien a recuperer, suppression legitime
    objet         {"events": [2 evenements valides]}
    partiel       [valide, evenement ampute, valide] + DEUXIEME passage
                  pour verifier qu'on ne reinsere pas deux fois
    base_kaput    2 evenements valides mais la base est corrompue au niveau
                  octet : le filet doit tenir, pas se vider

Regle jugee : un evenement en attente ne disparait qu'une fois ECRIT dans la
base. Sinon le fichier doit rester sur le disque.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_journal_pending_hostile"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_DS_FP = "tx_jph2_datasource"
_T0 = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc)


def _event_dict(idx: int) -> dict:
    """Un evenement en attente exactement dans la forme ecrite par le produit."""
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.integrity import _prepare_event_for_json

    evt = AuditEvent(
        event_id=idx,
        project_fingerprint="tx_jph2_project",
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="lyr_jph2",
        layer_name_snapshot="parcelles",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": idx}),
        operation_type="UPDATE",
        attributes_json=json.dumps(
            {"changed_only": {"proprio": {"old": "DUPONT", "new": "MARTIN"}}}),
        geometry_wkb=b"\x01\x01\x00\x00\x00pending" + str(idx).encode("ascii"),
        geometry_type="Point",
        crs_authid="EPSG:2154",
        field_schema_json=None,
        user_name="tester",
        session_id="sess_jph2",
        created_at=(_T0 + timedelta(seconds=idx)).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{idx}",
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )
    return _prepare_event_for_json(evt._asdict())


def _fresh_journal(tag: str) -> tuple:
    """Un journal SQLite initialise, WAL rabattu, dans son propre dossier."""
    from recoverland.core.sqlite_schema import initialize_schema

    tmpdir = tempfile.mkdtemp(prefix=f"rl_tx_jph2_{tag}_")
    db_path = str(Path(tmpdir) / "recoverland_audit.sqlite")
    conn = sqlite3.connect(db_path)
    initialize_schema(conn)
    conn.commit()
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass
    conn.close()
    for suffix in ("-wal", "-shm"):
        side = db_path + suffix
        if os.path.exists(side):
            try:
                os.remove(side)
            except OSError:
                pass
    return tmpdir, db_path


def _corrupt(db_path: str, offset: int, length: int) -> None:
    """Ecrase `length` octets a `offset` par un motif non nul."""
    with open(db_path, "r+b") as fh:
        fh.seek(offset)
        fh.write(bytes((i * 37 + 11) & 0xFF for i in range(length)))


def _write_pending(db_path: str, payload) -> str:
    from recoverland.core.integrity import _get_pending_path

    path = _get_pending_path(db_path)
    if isinstance(payload, bytes):
        with open(path, "wb") as fh:
            fh.write(payload)
    else:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
    return path


def _read_pending(path: str):
    if not os.path.isfile(path):
        return None
    raw = Path(path).read_bytes()
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {"__illisible__": len(raw)}


def _rows(db_path: str) -> list:
    try:
        conn = sqlite3.connect(db_path)
        try:
            return conn.execute(
                "SELECT entity_fingerprint, created_at FROM audit_event "
                "ORDER BY created_at"
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return []


def _probe(ctx, tag: str, payload, corrupt: bool = False,
           passes: int = 1) -> dict:
    from recoverland.core.integrity import check_journal_integrity
    from recoverland.core.logger import flog

    tmpdir, db_path = _fresh_journal(tag)
    if corrupt:
        # Page 1 : en-tete b-tree + sqlite_master. La base s'ouvre encore
        # mais aucune requete ne passe : c'est le pire moment pour un filet
        # de securite.
        _corrupt(db_path, 100, 400)

    pending_path = _write_pending(db_path, payload)
    before_bytes = os.path.getsize(pending_path)

    runs = []
    for _ in range(passes):
        result = check_journal_integrity(db_path, trace_id=ctx.trace_id)
        runs.append({
            "recovered": result.recovered_events,
            "rejected": result.pending_rejected,
            "healthy": result.is_healthy,
            "issues": list(result.issues),
        })

    res = {
        "tag": tag,
        "tmpdir": tmpdir,
        "db_path": db_path,
        "pending_bytes_before": before_bytes,
        "runs": runs,
        "pending_exists_after": os.path.isfile(pending_path),
        "pending_after": _read_pending(pending_path),
        "rows": _rows(db_path),
    }
    flog(
        f"tx_journal_pending_hostile case={tag}: trace_id={ctx.trace_id} "
        f"runs={runs} pending_exists_after={res['pending_exists_after']} "
        f"rows={len(res['rows'])}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["events"] = [_event_dict(1), _event_dict(2)]
    ampute = dict(_event_dict(3))
    ampute.pop("created_at", None)
    ampute.pop("user_name", None)
    ctx.data["ampute"] = ampute

    flog(
        f"tx_journal_pending_hostile setup: trace_id={ctx.trace_id} "
        f"n_events={len(ctx.data['events'])} "
        f"ampute_keys={sorted(ampute)[:4]}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"tx_journal_pending_hostile run start: trace_id={ctx.trace_id}", "INFO")

    e1, e2 = ctx.data["events"]
    ampute = ctx.data["ampute"]

    results = {}
    results["ordures"] = _probe(
        ctx, "ordures", b"{ceci n'est pas du json, le disque a lache")
    results["vide"] = _probe(ctx, "vide", b"")
    results["liste_vide"] = _probe(ctx, "liste_vide", [])
    results["objet"] = _probe(ctx, "objet", {"events": [e1, e2]})
    results["partiel"] = _probe(
        ctx, "partiel", [e1, ampute, e2], passes=2)
    results["base_kaput"] = _probe(
        ctx, "base_kaput", [e1, e2], corrupt=True)

    ctx.data["results"] = results

    flog(
        f"tx_journal_pending_hostile run end: trace_id={ctx.trace_id} "
        f"cases={sorted(results)}",
        "INFO",
    )


def _n_pending(res: dict) -> int:
    after = res.get("pending_after")
    if isinstance(after, list):
        return len(after)
    return -1


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    r = ctx.data.get("results") or {}

    # ===== 1. Fichier illisible : on garde, on ne devine pas =============
    ordures = r.get("ordures") or {}
    out.append((
        "ordures_fichier_conserve",
        ordures.get("pending_exists_after") is True,
        f"fichier d'attente illisible : conserve={ordures.get('pending_exists_after')}. "
        f"Tant qu'on n'a pas su le lire, le supprimer revient a jeter les "
        f"modifications de l'utilisateur sans les avoir regardees.",
    ))
    out.append((
        "ordures_rien_invente",
        (ordures.get("runs") or [{}])[0].get("recovered") == 0
        and not ordures.get("rows"),
        f"runs={ordures.get('runs')} rows={len(ordures.get('rows') or [])} : "
        f"un fichier illisible ne doit produire aucune ligne dans le journal",
    ))

    # ===== 2. Fichier vide (ecriture interrompue) =======================
    vide = r.get("vide") or {}
    out.append((
        "vide_fichier_conserve",
        vide.get("pending_exists_after") is True,
        f"fichier d'attente de 0 octet : conserve={vide.get('pending_exists_after')}. "
        f"Un fichier vide est la signature d'une ecriture coupee en plein "
        f"vol ; l'effacer ferme la seule piste restante.",
    ))

    # ===== 3. Liste vide : suppression legitime ==========================
    liste_vide = r.get("liste_vide") or {}
    out.append((
        "liste_vide_nettoyee",
        liste_vide.get("pending_exists_after") is False,
        f"'[]' ne contient rien a recuperer : le fichier doit etre supprime "
        f"pour ne pas etre relu a chaque demarrage "
        f"(conserve={liste_vide.get('pending_exists_after')})",
    ))

    # ===== 4. LE TROU : forme inattendue = destruction silencieuse ======
    objet = r.get("objet") or {}
    objet_run = (objet.get("runs") or [{}])[0]
    out.append((
        "objet_evenements_non_detruits",
        objet.get("pending_exists_after") is True
        or len(objet.get("rows") or []) == 2,
        f"fichier d'attente au format objet {{'events': [...]}} contenant 2 "
        f"evenements : recuperes={objet_run.get('recovered')} "
        f"lignes_en_base={len(objet.get('rows') or [])} "
        f"fichier_conserve={objet.get('pending_exists_after')}. "
        f"Le produit a supprime le fichier SANS lire une seule entree : les "
        f"2 modifications ne sont plus ni en base ni sur le disque. C'est le "
        f"filet de securite qui detruit ce qu'il devait sauver.",
    ))
    out.append((
        "objet_utilisateur_prevenu",
        objet_run.get("healthy") is False
        or any("pending" in i.lower() for i in objet_run.get("issues") or []),
        f"is_healthy={objet_run.get('healthy')} issues={objet_run.get('issues')} : "
        f"si le produit choisit malgre tout de jeter un fichier d'attente "
        f"qu'il ne sait pas lire, il doit au minimum le declarer, pas "
        f"annoncer un journal sain.",
    ))

    # ===== 5. Evenement ampute : trier, pas tout perdre ==================
    partiel = r.get("partiel") or {}
    runs = partiel.get("runs") or [{}, {}]
    out.append((
        "partiel_les_valides_sont_recuperes",
        runs[0].get("recovered") == 2,
        f"recuperes={runs[0].get('recovered')} attendu=2 : un evenement "
        f"ampute ne doit pas emporter avec lui les evenements sains du meme "
        f"fichier",
    ))
    out.append((
        "partiel_l_ampute_est_conserve",
        partiel.get("pending_exists_after") is True and _n_pending(partiel) == 1,
        f"fichier conserve={partiel.get('pending_exists_after')} "
        f"entrees_restantes={_n_pending(partiel)} attendu=1 : l'evenement "
        f"illisible doit rester disponible pour une reprise manuelle",
    ))
    out.append((
        "partiel_pas_de_double_insertion",
        len(partiel.get("rows") or []) == 2,
        f"apres DEUX demarrages consecutifs le journal contient "
        f"{len(partiel.get('rows') or [])} lignes, attendu 2 : un evenement "
        f"recupere deux fois serait compense deux fois par un REWIND",
    ))
    out.append((
        "partiel_second_demarrage_stable",
        len(runs) > 1 and runs[1].get("recovered") == 0,
        f"second passage : recuperes={runs[1].get('recovered') if len(runs) > 1 else None} "
        f"attendu=0",
    ))

    # ===== 6. Base corrompue : le filet doit tenir ======================
    kaput = r.get("base_kaput") or {}
    kaput_run = (kaput.get("runs") or [{}])[0]
    out.append((
        "base_kaput_detectee",
        kaput_run.get("healthy") is False and bool(kaput_run.get("issues")),
        f"base corrompue au niveau octet : is_healthy={kaput_run.get('healthy')} "
        f"issues={kaput_run.get('issues')}. Une corruption non detectee laisse "
        f"le plugin continuer a ecrire dans une base morte.",
    ))
    out.append((
        "base_kaput_attente_preservee",
        kaput.get("pending_exists_after") is True
        and _n_pending(kaput) == 2,
        f"fichier d'attente conserve={kaput.get('pending_exists_after')} "
        f"entrees={_n_pending(kaput)} attendu=2 : quand la base ne peut plus "
        f"rien accepter, les evenements en attente sont la SEULE copie "
        f"survivante ; ils doivent rester sur le disque",
    ))
    out.append((
        "base_kaput_rien_annonce_a_tort",
        kaput_run.get("recovered") == 0,
        f"recuperes={kaput_run.get('recovered')} attendu=0 : annoncer une "
        f"recuperation qui n'a pas eu lieu conduit l'utilisateur a croire son "
        f"historique complet",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_pending_hostile.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
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
