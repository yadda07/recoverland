# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_serial_lone_surrogate_capture - RecoverLand validation runtime.

Invariant: I-1 (toute modification capturee atteint le journal ou un
repli durable ; une valeur invalide ne detruit jamais d'autres evenements).

Le trou que ce scenario prouve
------------------------------
Les chaines Qt sont en UTF-16. Une expression QGIS qui coupe un texte en
unites UTF-16 (``left("src", 4)`` en valeur par defaut appliquee a la mise
a jour, le calculateur de champ, une troncature a la longueur du champ)
peut couper un emoji en deux et laisser la MOITIE d'une paire de
substitution. Le tampon d'edition la transmet a Python telle quelle
(``'abc\\ud83d'``). `serialize_value` la recopiait, les charges utiles sont
construites par ``json.dumps(ensure_ascii=False)``, et SQLite ne peut pas
encoder un demi-caractere en UTF-8 : l'ecriture leve UnicodeEncodeError.
Ce n'est pas une sqlite3.Error : TOUT le lot part dans le repli, dont
l'ecriture JSON leve la meme erreur. Le lot est perdu, evenements sains
compris.

Cote donnees, le provider n'a pas ce probleme : il ecrit U+FFFD a la place
du demi-caractere (verifie sur GeoPackage) et le commit reussit.
L'utilisateur voit sa modification enregistree ; le journal, lui, a perdu
ce commit et tout ce qui etait dans le meme lot, sans rien dire.

Dispositif : une vraie GeoPackage, le vrai tracker, la vraie file
d'ecriture et un vrai journal. Le champ ``t`` porte la valeur par defaut
``left("src", 4)`` appliquee a chaque mise a jour. L'utilisateur modifie le
champ ``n`` de deux entites dans UN commit : pour A (src = 'abc' + emoji)
``t`` devient 'abc' + demi-emoji, pour B (src = 'xyz') ``t`` reste sain.

Verdict attendu avant correctif : FAIL (les deux evenements manquent).
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_serial_lone_surrogate_capture"
INVARIANT = "I-1"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_SRC_A = "abc\U0001F600"
_EXPECTED_T = "abc�"


def _make_gpkg(tmpdir):
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
        QgsFeature, QgsGeometry, QgsPointXY,
    )
    mem = QgsVectorLayer("Point?crs=EPSG:4326&field=nom:string(10)"
                         "&field=src:string(0)&field=t:string(0)"
                         "&field=n:integer", "seed", "memory")
    mem.startEditing()
    for nom, src, x in (("A", _SRC_A, 1.0), ("B", "xyz", 2.0)):
        feat = QgsFeature(mem.fields())
        feat.setAttributes([nom, src, "init", 0])
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, x)))
        mem.addFeature(feat)
    mem.commitChanges()
    path = os.path.join(tmpdir, "surrogate.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "zones"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    layer = QgsVectorLayer(f"{path}|layername=zones", "zones_surrogate", "ogr")
    if not layer.isValid():
        raise RuntimeError("couche GPKG invalide")
    return layer


def _fid(layer, nom):
    for feat in layer.getFeatures():
        if feat["nom"] == nom:
            return feat.id()
    return None


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_serial_lone_surrogate_capture setup: trace_id={ctx.trace_id}",
         "INFO")


def run(ctx):
    from qgis.core import QgsDefaultValue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    from scripts.validation.scenarios.tx_prov_fingerprint_split import (
        _open_journal, _WriteQueueBarrier, _journal_row_count,
    )

    d = ctx.data
    d["error"] = None
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_surr_")
    jrnl = tempfile.mkdtemp(prefix="rl_tx_surr_j_")
    jm = wq = None
    try:
        layer = _make_gpkg(tmpdir)
        idx_t = layer.fields().indexOf("t")
        idx_n = layer.fields().indexOf("n")
        layer.setDefaultValueDefinition(idx_t, QgsDefaultValue('left("src", 4)', True))
        jm, wq = _open_journal(jrnl)
        barrier = _WriteQueueBarrier(wq)
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.connect_layer(layer)
        d["connected"] = layer.id() in tracker._connected_layers
        d["rows_before"] = _journal_row_count(jm)

        fid_a, fid_b = _fid(layer, "A"), _fid(layer, "B")
        layer.startEditing()
        layer.changeAttributeValue(fid_a, idx_n, 1)
        layer.changeAttributeValue(fid_b, idx_n, 2)
        buffered = layer.editBuffer().changedAttributeValues()
        d["buffer_t_a"] = (buffered.get(fid_a) or {}).get(idx_t)
        d["commit_ok"] = bool(layer.commitChanges())
        d["drained"] = barrier.wait(jm, timeout_s=8.0)
        d["accepted"] = barrier.accepted

        layer.reload()
        stored = {f["nom"]: f["t"] for f in layer.getFeatures()}
        d["stored_t_a"] = stored.get("A")
        d["stored_t_b"] = stored.get("B")

        fp = compute_datasource_fingerprint(layer)
        conn = jm.create_read_connection()
        try:
            rows = conn.execute(
                "SELECT operation_type, attributes_json FROM audit_event "
                "WHERE datasource_fingerprint = ? ORDER BY event_id",
                (fp,)).fetchall()
        finally:
            conn.close()
        import json
        d["events"] = [(op, json.loads(attrs)) for op, attrs in rows]
        try:
            tracker.disconnect_all()
        except Exception:  # noqa: BLE001
            pass
        layer = None
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        try:
            if wq is not None:
                wq.stop()
        except Exception:  # noqa: BLE001
            pass
        try:
            if jm is not None and jm.is_open:
                jm.close()
        except Exception:  # noqa: BLE001
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(jrnl, ignore_errors=True)
    flog(f"tx_serial_lone_surrogate_capture run: trace_id={ctx.trace_id} "
         f"buffer_t_a={d.get('buffer_t_a')!r} stored_t_a={d.get('stored_t_a')!r} "
         f"accepted={d.get('accepted')} events={len(d.get('events') or [])} "
         f"error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def _t_new(events, n_value):
    for op, attrs in events:
        changed = (attrs or {}).get("changed_only") or {}
        if (changed.get("n") or {}).get("new") == n_value:
            return op, (changed.get("t") or {}).get("new", "<absent>")
    return None, None


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    events = d.get("events") or []
    op_a, t_a = _t_new(events, 1)
    op_b, _t_b = _t_new(events, 2)
    return [
        ("scenario_ran", d.get("error") is None,
         f"le scenario a plante : {d.get('error')}"),
        ("fixture_produces_a_lone_surrogate",
         d.get("buffer_t_a") == "abc\ud83d" and d.get("commit_ok") is True,
         f"tampon t(A)={d.get('buffer_t_a')!r} commit={d.get('commit_ok')} : "
         f"le dispositif doit reellement produire un demi-caractere, sinon il "
         f"ne teste rien."),
        ("provider_stores_the_replacement_char",
         d.get("stored_t_a") == _EXPECTED_T,
         f"valeur stockee par la GeoPackage pour t(A)={d.get('stored_t_a')!r} "
         f"(attendu {_EXPECTED_T!r}) : c'est la reference que le journal "
         f"doit porter."),
        ("healthy_event_of_the_batch_survives",
         op_b == "UPDATE",
         f"evenements journalises={events} : la modification de B, parfaitement "
         f"saine, a disparu avec son lot a cause d'un demi-caractere dans A."),
        ("event_with_the_bad_value_is_journaled",
         op_a == "UPDATE",
         f"evenements journalises={events} : la modification de A n'est pas "
         f"dans le journal alors que le provider l'a acceptee ; le rewind ne "
         f"pourra jamais la defaire."),
        ("journal_holds_what_the_provider_stored",
         t_a == d.get("stored_t_a") == _EXPECTED_T,
         f"t(A) journalise={t_a!r} stocke={d.get('stored_t_a')!r} : le journal "
         f"doit porter la valeur que la source contient reellement, sinon "
         f"chaque preuve de restauration compare deux valeurs differentes."),
        assert_log_contains(
            ctx.records,
            rf"tx_serial_lone_surrogate_capture run: trace_id={ctx.trace_id}",
            name="trace_id_propagated"),
    ]


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
