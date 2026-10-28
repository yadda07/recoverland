# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_crs_relabel_vs_reprojection - RecoverLand validation runtime.

Invariant: I-3 (l'ecriture doit viser la bonne entite, au bon endroit).
Constat resolu: R-33 (HIGH), un changement d'etiquette de SCR traite comme
une reprojection: la restauration teleporte les entites.

Le mecanisme
------------
Le journal garde le WKB octet a octet: les coordonnees BRUTES que le
fournisseur de donnees a rendues au moment de la capture. `event.crs_authid`
n'est que le SCR que la couche DECLARAIT a cet instant. Quand ce SCR differe
du SCR actuel de la couche, deux histoires sont possibles, et elles exigent
deux ecritures opposees:

  * changement d'etiquette (relabel): "Definir le SCR de la couche", un
    .prj corrige, un SCR mal declare puis rectifie. Le fournisseur rend
    toujours les memes nombres; seule leur interpretation a change. Il faut
    ecrire les coordonnees brutes telles quelles.
  * reprojection en place: le fichier a ete reecrit dans un autre SCR au
    meme chemin. Les nombres du fournisseur ont change. Il faut reprojeter.

`geometry_utils.geometry_for_layer_crs` reprojetait des que les deux codes
differaient, sans rien regarder d'autre. Pour une meme source, un SCR
different est le plus souvent un changement d'etiquette: la reprojection
deplace alors chaque entite restauree (des centaines ou des milliers de
kilometres), et la restauration annonce un succes. Quand la reprojection
est impossible (des metres Lambert-93 lus comme des degres), tout est
refuse: l'utilisateur qui a corrige un .prj ne peut plus rien restaurer.

Deuxieme passe (v7): l'emprise n'est pas une preuve
---------------------------------------------------
La premiere version du correctif departageait, quand la zone de validite
ne disait rien, par l'emprise des donnees restantes, elargie d'au moins
1 % de la zone du SCR (~400 km en EPSG:3857). Etre pres des AUTRES entites
ne dit pas ce qui est arrive aux coordonnees: Lambert-93 et Web Mercator se
recouvrent numeriquement sur la France. Un fichier Lambert-93 reecrit en
Web Mercator dont le reste est a Paris: l'entite supprimee a Toulouse etait
recreee a 968 km ("Restored"), celle de Bordeaux a 956 km. La decision
etait aussi prise geometrie par geometrie: l'identite d'un UPDATE etait
prouvee sous une histoire, l'ancienne geometrie ecrite sous l'autre. Et une
decision "ambigue" devenait, pour les preuves d'identite, "pas de geometrie
a comparer": l'annulation d'un INSERT supprimait une entite sans rapport
qui portait les memes valeurs a 3 km.

Le correctif attendu decide sur preuves, une fois par (couche, SCR de
capture), et la meme decision pour les deux moteurs (rewind
`restore_executor._geometry_in_layer_crs`, restauration evenement par
evenement `restore_service._geometry_for_write`) et pour les preuves
d'identite (`restore_service._expected_geometry`):
  (a) un temoin: une entite encore dans la couche porte EXACTEMENT une
      lecture d'une geometrie capturee de l'evenement (la sienne, ou la
      NOUVELLE geometrie d'un UPDATE, que la cible porte encore). Brute:
      etiquette; reprojetee: reprojection;
  (b) la zone de validite du SCR de la couche, categorique seulement
      (position rejetee a plus d'une taille de zone, ou reprojection
      impossible); un verdict de zone faible ne vaut que corrobore;
  (c) la decision prise pour (couche, SCR de capture) est gardee: une
      geometrie sans preuve propre la suit, une preuve qui la contredit la
      rend conflictuelle (refus);
  (d) sinon refus NOMME (reason_code=crs_ambiguous), rien d'ecrit. Chaque
      decision est journalisee avec sa preuve;
  (e) une decision ambigue ne devient jamais "pas de geometrie a comparer"
      pour les preuves d'identite: elles comparent a la lecture
      reprojetee, qu'aucune entite ne porte exactement (sinon elle aurait
      ete temoin et aurait fixe l'histoire). Une preuve qui repose sur la
      geometrie echoue donc au lieu d'etre sautee, et le garde-fou du
      sauvetage par attributs (entite proche de la geometrie capturee)
      reste actif.

Ce que l'utilisateur voit sans le correctif
-------------------------------------------
Il corrige le SCR d'une couche (Lambert-93 declare en CC46 par erreur) puis
lance un rewind: toutes les entites restaurees partent a 1 600 km, en mer
du Nord, et le rewind affiche "Reverted". Ou il corrige un .prj WGS84 d'un
fichier Lambert-93: chaque restauration est refusee "cannot be converted".
Ou sa couche Web Mercator mal declaree (des degres) est corrigee en
EPSG:4326: l'entite restauree atterrit sur l'ile nulle (0, 0), avec succes.
Ou il reprojette son fichier national en Web Mercator: l'entite de
Toulouse revient a 968 km, pres de Verdun, avec succes.

Cas (vrais GeoPackages, vrai fournisseur OGR, relecture du fournisseur;
chaque cas est joue sur deux copies identiques, une par moteur, dans le
MEME ordre: "del_first" est l'ordre du rewind, qui restaure les
suppressions avant les modifications)
    relabel_prj          fichier Lambert-93 dont le SCR declare etait
                         EPSG:4326, corrige en EPSG:2154 au meme chemin.
                         Attendu: coordonnees brutes (reprojection
                         impossible: des metres lus comme des degres).
    relabel_setcrs       couche Lambert-93 reglee par erreur en EPSG:3946
                         (Definir le SCR de la couche) pendant la capture,
                         puis remise en EPSG:2154. UPDATE: brut (la cible
                         est temoin). DELETE joue d'abord: la zone ne donne
                         qu'un verdict faible, rien ne le corrobore: refus.
    relabel_setcrs_story_first
                         meme decor, UPDATE joue d'abord: sa cible fixe
                         l'histoire (etiquette), la suppression la suit et
                         revient brute.
    reprojection         capture en degres EPSG:4326, fichier reecrit en
                         EPSG:2154 au meme chemin. Attendu: reprojete.
    ambiguous_empty      degres declares EPSG:3857, corriges en EPSG:4326,
                         couche vide: rien ne departage. Attendu: refus
                         crs_ambiguous, rien d'ecrit.
    extent_relabel       meme SCR que ambiguous_empty, la couche garde des
                         entites. UPDATE brut (temoin). DELETE joue
                         d'abord: la proximite des autres entites n'est pas
                         une preuve, refus.
    extent_relabel_story_first / extent_reprojection_story_first
                         UPDATE d'abord: la suppression suit l'histoire.
    extent_reprojection  capture EPSG:4326, fichier reecrit en EPSG:3857
                         avec des entites. UPDATE reprojete (temoin),
                         DELETE d'abord: refus.
    area_extent_conflict donnees de Lyon capturees en EPSG:4326 puis
                         reecrites en EPSG:27700 (grille britannique): les
                         degres bruts tombent DANS la zone britannique, la
                         position reprojetee dehors, mais a moins d'une
                         taille de zone: verdict faible, non corrobore,
                         refus crs_ambiguous, rien d'ecrit.
    bng_lyon_empty       meme chose, couche vide (sonde du relecteur):
                         refus, jamais des degres ecrits comme des metres.
    stray_feature_strong_area
                         capture EPSG:4326, fichier reecrit en EPSG:2154
                         dont la seule autre entite est un point parasite a
                         (0, 0). La zone est categorique: reprojete.
    equivalent_ids       couche EPSG:4326 redeclaree OGC:CRS84: deux
                         identifiants, une definition. Ecriture telle
                         quelle, jamais un refus "ambigu".
    l93_3857_toulouse / l93_3857_bordeaux
                         fichier Lambert-93 reecrit en EPSG:3857 au meme
                         chemin, le reste des donnees a Paris, l'entite
                         supprimee a Toulouse / Bordeaux (sondes du
                         relecteur). La zone ne departage pas (les deux
                         lectures sont en France): refus, jamais la
                         position brute a ~960 km.
    l93_3857_toulouse_story_first
                         meme decor plus un UPDATE a Paris joue d'abord:
                         sa cible fixe l'histoire (reprojection), Toulouse
                         revient a sa vraie place.
    l93_3857_update_moved_back
                         l'entite de Toulouse deplacee a Paris (UPDATE),
                         puis reprojection en place. L'identite et
                         l'ecriture suivent la meme histoire: retour a
                         Toulouse reprojete, pas a 968 km.
    relabel_3857_l93_far fichier Lambert-93 regle par erreur en EPSG:3857
                         a la capture, corrige en EPSG:2154; le reste pres
                         de Verdun, ou tombe la lecture reprojetee de
                         Toulouse: refus, jamais Verdun.
    relabel_3857_l93_far_story_first
                         meme decor plus un UPDATE pres de Verdun joue
                         d'abord: histoire etiquette, Toulouse revient brut.
    story_conflict       deux entites de la meme couche racontent deux
                         histoires (A: lecture brute, D: lecture
                         reprojetee). A fixe l'histoire (etiquette); D la
                         contredit: D et la suppression qui suit sont
                         refusees, jamais ecrites sous l'autre histoire.
    rejeu              chaque restauration unitaire d'un DELETE est
                         relancee aussitot: la recherche de doublon suit la
                         meme decision que l'ecriture (skip, ou refus
                         renouvele), jamais une seconde copie.
    insert_undo_ambiguous
                         annulation d'un INSERT (une SUPPRESSION) alors que
                         l'histoire est indecise: l'entite inseree n'est
                         plus la, une entite sans rapport portant les memes
                         valeurs ("borne", "LYON") est a 3 km. Attendu: refus
                         nomme, rien de supprime.
    insert_undo_witness  temoin positif: l'entite inseree est encore la
                         (reprojetee avec le fichier). Elle est supprimee,
                         la voisine reste.

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import gc
import json
import math
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rs_crs_relabel_vs_reprojection"
INVARIANT = "I-3"

# Expected log signature documented for grep-ability:
EXPECTED_SIGNATURE = (r"crs_decision decision=(relabel|reprojection|ambiguous) "
                      r"src_crs=\S+ layer_crs=\S+ evidence=\S+")

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FIELDS = [{"name": "nom", "type": "String", "length": 32, "precision": 0}]

# A: deplacee de 50 m (UPDATE a restaurer), B: supprimee (DELETE a
# restaurer), C: temoins jamais touches. Coordonnees BRUTES du fichier.
_L93_A = ((652000.0, 6862000.0), (652050.0, 6862050.0))
_L93_B = (653000.0, 6863000.0)
_DEG_A = ((2.35, 48.85), (2.3505, 48.8505))
_DEG_B = (2.36, 48.86)
_DEG_C = (2.3522, 48.8566)
# Sondes du relecteur: le reste des donnees a Paris (Lambert-93), la
# suppression loin de lui.
_PARIS_L93 = [(652000.0, 6862000.0), (655000.0, 6860000.0),
              (648000.0, 6865000.0)]
_TOULOUSE_L93 = (574500.0, 6279500.0)
_BORDEAUX_L93 = (417000.0, 6421000.0)
# Pres de Verdun: la ou tombe Toulouse (Lambert-93) lu comme du Web
# Mercator puis reprojete en Lambert-93.
_VERDUN_L93 = [(868000.0, 6893000.0), (880000.0, 6900000.0),
               (860000.0, 6885000.0)]

# Hypothesis revised (v7): relabel_setcrs, extent_relabel and
# extent_reprojection expected their DELETE to be written (raw /
# reprojected) with the rewind order (DELETE first). The only evidence for
# it was the data extent grown by at least 1 % of the CRS area (plus a weak
# area verdict for relabel_setcrs). The reviewer's Toulouse probe shows that
# being near the OTHER features is not evidence of the story (968 km off,
# reported "Restored"), so with nothing else the DELETE is refused, named
# and written nowhere (checked below exactly like the other refusals). The
# write itself stays covered, and more strictly, by the *_story_first
# variants: there the position must follow the story that the UPDATE's own
# target established, in both engines.
_CASES = {
    "relabel_prj": {
        "file_epsg": 4326, "capture_crs": None,
        "a": _L93_A, "b": _L93_B, "c": [],
        "change": ("prj", 2154), "order": "del_first",
        "expect": {"upd": "raw", "del": "raw"},
        "story": "fichier Lambert-93 declare EPSG:4326, .prj corrige en "
                 "EPSG:2154",
    },
    "relabel_setcrs": {
        "file_epsg": 2154, "capture_crs": "EPSG:3946",
        "a": _L93_A, "b": _L93_B, "c": [],
        "change": ("setcrs", 2154), "order": "del_first",
        "expect": {"upd": "raw", "del": "refused"},
        "story": "couche Lambert-93 reglee par erreur en EPSG:3946, puis "
                 "remise en EPSG:2154 (Definir le SCR de la couche), "
                 "suppression restauree d'abord",
    },
    "relabel_setcrs_story_first": {
        "file_epsg": 2154, "capture_crs": "EPSG:3946",
        "a": _L93_A, "b": _L93_B, "c": [],
        "change": ("setcrs", 2154), "order": "upd_first",
        "expect": {"upd": "raw", "del": "raw"},
        "story": "couche Lambert-93 reglee par erreur en EPSG:3946, puis "
                 "remise en EPSG:2154, modification restauree d'abord",
    },
    "reprojection": {
        "file_epsg": 4326, "capture_crs": None,
        "a": _DEG_A, "b": _DEG_B, "c": [],
        "change": ("reproject", 2154), "order": "del_first",
        "expect": {"upd": "reprojected", "del": "reprojected"},
        "story": "capture en degres EPSG:4326, fichier reecrit en EPSG:2154 "
                 "au meme chemin",
    },
    "ambiguous_empty": {
        "file_epsg": 3857, "capture_crs": None,
        "a": None, "b": _DEG_B, "c": [],
        "change": ("prj", 4326), "order": "del_first",
        "expect": {"del": "refused"},
        "story": "degres declares EPSG:3857, corriges en EPSG:4326, couche "
                 "vide",
    },
    "extent_relabel": {
        "file_epsg": 3857, "capture_crs": None,
        "a": _DEG_A, "b": _DEG_B, "c": [_DEG_C],
        "change": ("prj", 4326), "order": "del_first",
        "expect": {"upd": "raw", "del": "refused"},
        "story": "degres declares EPSG:3857, corriges en EPSG:4326, la couche "
                 "garde des entites, suppression restauree d'abord",
    },
    "extent_relabel_story_first": {
        "file_epsg": 3857, "capture_crs": None,
        "a": _DEG_A, "b": _DEG_B, "c": [_DEG_C],
        "change": ("prj", 4326), "order": "upd_first",
        "expect": {"upd": "raw", "del": "raw"},
        "story": "degres declares EPSG:3857, corriges en EPSG:4326, "
                 "modification restauree d'abord",
    },
    "extent_reprojection": {
        "file_epsg": 4326, "capture_crs": None,
        "a": _DEG_A, "b": _DEG_B, "c": [_DEG_C],
        "change": ("reproject", 3857), "order": "del_first",
        "expect": {"upd": "reprojected", "del": "refused"},
        "story": "capture EPSG:4326, fichier reecrit en EPSG:3857 avec des "
                 "entites, suppression restauree d'abord",
    },
    "extent_reprojection_story_first": {
        "file_epsg": 4326, "capture_crs": None,
        "a": _DEG_A, "b": _DEG_B, "c": [_DEG_C],
        "change": ("reproject", 3857), "order": "upd_first",
        "expect": {"upd": "reprojected", "del": "reprojected"},
        "story": "capture EPSG:4326, fichier reecrit en EPSG:3857, "
                 "modification restauree d'abord",
    },
    "area_extent_conflict": {
        "file_epsg": 4326, "capture_crs": None,
        "a": None, "b": (4.85, 45.76), "c": [(4.8357, 45.764)],
        "change": ("reproject", 27700), "order": "del_first",
        "expect": {"del": "refused"},
        "story": "Lyon capture en EPSG:4326, fichier reecrit en EPSG:27700",
    },
    "bng_lyon_empty": {
        "file_epsg": 4326, "capture_crs": None,
        "a": None, "b": (4.85, 45.76), "c": [],
        "change": ("reproject", 27700), "order": "del_first",
        "expect": {"del": "refused"},
        "story": "Lyon capture en EPSG:4326, fichier reecrit en EPSG:27700, "
                 "couche vide",
    },
    "stray_feature_strong_area": {
        "file_epsg": 4326, "capture_crs": None,
        "a": None, "b": _DEG_B, "c": [],
        "after_extra": [(9, "PARASITE", (0.0, 0.0))],
        "change": ("reproject", 2154), "order": "del_first",
        "expect": {"del": "reprojected"},
        "story": "capture EPSG:4326, fichier reecrit en EPSG:2154 dont la "
                 "seule autre entite est un point parasite a (0, 0)",
    },
    "equivalent_ids": {
        "file_epsg": 4326, "capture_crs": None,
        "a": _DEG_A, "b": _DEG_B, "c": [],
        "change": ("setcrs", "OGC:CRS84"), "order": "del_first",
        "same_position": True,
        "expect": {"upd": "raw", "del": "raw"},
        "story": "couche EPSG:4326 redeclaree OGC:CRS84 (deux identifiants, "
                 "une meme definition)",
    },
    "l93_3857_toulouse": {
        "file_epsg": 2154, "capture_crs": None,
        "a": None, "b": _TOULOUSE_L93, "c": _PARIS_L93,
        "change": ("reproject", 3857), "order": "del_first",
        "expect": {"del": "refused"},
        "story": "fichier Lambert-93 reecrit en EPSG:3857 au meme chemin, "
                 "reste a Paris, suppression a Toulouse",
    },
    "l93_3857_bordeaux": {
        "file_epsg": 2154, "capture_crs": None,
        "a": None, "b": _BORDEAUX_L93, "c": _PARIS_L93,
        "change": ("reproject", 3857), "order": "del_first",
        "expect": {"del": "refused"},
        "story": "fichier Lambert-93 reecrit en EPSG:3857 au meme chemin, "
                 "reste a Paris, suppression a Bordeaux",
    },
    "l93_3857_toulouse_story_first": {
        "file_epsg": 2154, "capture_crs": None,
        "a": ((651000.0, 6861000.0), (651050.0, 6861050.0)),
        "b": _TOULOUSE_L93, "c": _PARIS_L93,
        "change": ("reproject", 3857), "order": "upd_first",
        "expect": {"upd": "reprojected", "del": "reprojected"},
        "story": "fichier Lambert-93 reecrit en EPSG:3857, reste a Paris, "
                 "une modification a Paris restauree avant la suppression "
                 "de Toulouse",
    },
    "l93_3857_update_moved_back": {
        "file_epsg": 2154, "capture_crs": None,
        "a": (_TOULOUSE_L93, (650000.0, 6861000.0)),
        "b": None, "c": _PARIS_L93,
        "change": ("reproject", 3857), "order": "del_first",
        "expect": {"upd": "reprojected"},
        "story": "entite de Toulouse deplacee a Paris (UPDATE), fichier "
                 "Lambert-93 reecrit en EPSG:3857",
    },
    "relabel_3857_l93_far": {
        "file_epsg": 2154, "capture_crs": "EPSG:3857",
        "a": None, "b": _TOULOUSE_L93, "c": _VERDUN_L93,
        "change": ("setcrs", 2154), "order": "del_first",
        "expect": {"del": "refused"},
        "story": "fichier Lambert-93 regle par erreur en EPSG:3857 a la "
                 "capture, corrige en EPSG:2154, reste pres de Verdun, "
                 "suppression a Toulouse",
    },
    "relabel_3857_l93_far_story_first": {
        "file_epsg": 2154, "capture_crs": "EPSG:3857",
        "a": ((870000.0, 6895000.0), (870050.0, 6895050.0)),
        "b": _TOULOUSE_L93, "c": _VERDUN_L93,
        "change": ("setcrs", 2154), "order": "upd_first",
        "expect": {"upd": "raw", "del": "raw"},
        "story": "fichier Lambert-93 regle par erreur en EPSG:3857, corrige "
                 "en EPSG:2154, une modification pres de Verdun restauree "
                 "avant la suppression de Toulouse",
    },
    # Deux entites de la meme couche racontent deux histoires: A porte la
    # lecture brute de sa nouvelle geometrie (etiquette), D la lecture
    # reprojetee de la sienne (comme si elle seule avait ete reprojetee).
    # La premiere fixe l'histoire; la seconde la contredit: D et tout ce qui
    # suit sont refuses, jamais ecrits sous l'autre histoire.
    "story_conflict": {
        "file_epsg": 2154, "capture_crs": "EPSG:3946",
        "a": _L93_A, "b": _L93_B, "c": [],
        "d": ((654000.0, 6864000.0), (654050.0, 6864050.0)),
        "forge_d": "reprojected",
        "change": ("setcrs", 2154), "order": ("upd", "upd2", "del"),
        "expect": {"upd": "raw", "upd2": "refused", "del": "refused"},
        "story": "couche Lambert-93 reglee par erreur en EPSG:3946 puis remise "
                 "en EPSG:2154; une seconde entite porte la lecture "
                 "reprojetee de sa geometrie",
    },
}
_PATHS = ("svc", "rw")
_PATH_LABEL = {"svc": "restauration evenement par evenement",
               "rw": "rewind (tampon d'edition)"}

_EXPECTED_CRS = {
    "relabel_prj": ("EPSG:4326", "EPSG:2154"),
    "relabel_setcrs": ("EPSG:3946", "EPSG:2154"),
    "relabel_setcrs_story_first": ("EPSG:3946", "EPSG:2154"),
    "reprojection": ("EPSG:4326", "EPSG:2154"),
    "ambiguous_empty": ("EPSG:3857", "EPSG:4326"),
    "extent_relabel": ("EPSG:3857", "EPSG:4326"),
    "extent_relabel_story_first": ("EPSG:3857", "EPSG:4326"),
    "extent_reprojection": ("EPSG:4326", "EPSG:3857"),
    "extent_reprojection_story_first": ("EPSG:4326", "EPSG:3857"),
    "area_extent_conflict": ("EPSG:4326", "EPSG:27700"),
    "bng_lyon_empty": ("EPSG:4326", "EPSG:27700"),
    "stray_feature_strong_area": ("EPSG:4326", "EPSG:2154"),
    "equivalent_ids": ("EPSG:4326", "OGC:CRS84"),
    "l93_3857_toulouse": ("EPSG:2154", "EPSG:3857"),
    "l93_3857_bordeaux": ("EPSG:2154", "EPSG:3857"),
    "l93_3857_toulouse_story_first": ("EPSG:2154", "EPSG:3857"),
    "l93_3857_update_moved_back": ("EPSG:2154", "EPSG:3857"),
    "relabel_3857_l93_far": ("EPSG:3857", "EPSG:2154"),
    "relabel_3857_l93_far_story_first": ("EPSG:3857", "EPSG:2154"),
    "story_conflict": ("EPSG:3946", "EPSG:2154"),
}

# Annulation d'un INSERT (sonde du relecteur): des postes sur toute la
# France, Y ("borne", "LYON") sans rapport, X ("borne", "LYON") insere a
# 3 km de Y. Fichier reecrit en place de EPSG:2154 en EPSG:3857.
_IU_NAMES = ("nom", "commune")
_IU_FIELDS = [{"name": name, "type": "String", "length": 32, "precision": 0}
              for name in _IU_NAMES]
_IU_KEEP = [(1, ("poste", "PARIS"), (652000.0, 6862000.0)),
            (2, ("poste", "LILLE"), (704000.0, 7059000.0)),
            (3, ("poste", "MARSEILLE"), (893000.0, 6247000.0)),
            (4, ("poste", "BREST"), (146000.0, 6838000.0)),
            (5, ("borne", "LYON"), (845000.0, 6520000.0))]
_IU_Y = 5
_IU_X = (20, ("borne", "LYON"), (842000.0, 6519000.0))
_INSERT_UNDO = {
    "insert_undo_ambiguous": {"x_still_there": False},
    "insert_undo_witness": {"x_still_there": True},
}


def _wkb_point(x, y) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb())


def _write_gpkg(path: str, epsg, rows, names=("nom",)) -> None:
    """GeoPackage mono-couche "reseau" (Point, champs *names*), fids imposes.

    *rows*: (fid, valeurs, wkb) -- *valeurs* une chaine (champ nom seul) ou
    un tuple aligne sur *names*. Les octets sont ecrits tels quels, sans la
    moindre interpretation: c'est ce que fait un .prj corrige.
    """
    from osgeo import ogr, osr

    drv = ogr.GetDriverByName("GPKG")
    ds = drv.CreateDataSource(path)
    if ds is None:
        raise RuntimeError(f"GeoPackage non cree: {path}")
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(int(epsg))
    lyr = ds.CreateLayer("reseau", srs, ogr.wkbPoint)
    for name in names:
        field = ogr.FieldDefn(name, ogr.OFTString)
        field.SetWidth(32)
        lyr.CreateField(field)
    for fid, values, wkb in rows:
        if isinstance(values, str):
            values = (values,)
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetFID(int(fid))
        for name, value in zip(names, values):
            feat.SetField(name, value)
        feat.SetGeometry(ogr.CreateGeometryFromWkb(bytes(wkb)))
        if lyr.CreateFeature(feat) != 0:
            raise RuntimeError(f"entite {fid} non ecrite dans {path}")
    ds = None


def _open(path: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername=reseau", "reseau", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche GPKG invalide: {path}")
    return layer


def _release(layer) -> None:
    """Ferme les connexions OGR mises en commun sur le fichier de *layer*.

    Sans cela Windows garde le GeoPackage verrouille et le fichier ne peut
    pas etre reecrit au meme chemin.
    """
    if layer is None:
        return
    try:
        layer.dataProvider().reloadData()
    except (AttributeError, RuntimeError):
        pass


def _rewrite(holder: list, path: str, new_epsg, reproject: bool, extra=(),
             names=("nom",)):
    """Reecrit le fichier de la couche au meme chemin, SCR *new_epsg*.

    *holder*: liste contenant la seule reference a la couche (l'appelant
    n'en garde aucune: une reference vivante garde le fichier verrouille
    sous Windows). *reproject*: coordonnees converties (reprojection en
    place), sinon les memes nombres (un .prj corrige). Renvoie la couche
    rouverte.
    """
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsGeometry,
        QgsProject,
    )
    layer = holder.pop()
    new_crs = QgsCoordinateReferenceSystem(
        new_epsg if isinstance(new_epsg, str) else f"EPSG:{new_epsg}")
    provider = layer.dataProvider()
    live = [(f.id(), tuple(f[name] for name in names), QgsGeometry(f.geometry()))
            for f in provider.getFeatures()]
    if reproject:
        xform = QgsCoordinateTransform(layer.crs(), new_crs,
                                       QgsProject.instance())
        for _fid, _values, geom in live:
            geom.transform(xform)
    _release(layer)
    provider = None
    layer = None
    gc.collect()
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    rows = [(fid, values, bytes(geom.asWkb())) for fid, values, geom in live]
    # Entites deja presentes dans le fichier reecrit (coordonnees du
    # nouveau SCR), par exemple un point parasite a (0, 0).
    rows += [(fid, values, _wkb_point(*xy)) for fid, values, xy in extra]
    _write_gpkg(path, new_epsg, rows, names)
    return _open(path)


def _feature(layer, fid):
    from qgis.core import QgsFeatureRequest
    return next(layer.dataProvider().getFeatures(QgsFeatureRequest(int(fid))),
                None)


def _event(operation, fid, geom_wkb, new_geom_wkb, crs, attrs_json,
           event_id, fields=None):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.sqlite_schema import CURRENT_SCHEMA_VERSION

    return AuditEvent(
        event_id=event_id,
        project_fingerprint="rs_crs_rel_project",
        datasource_fingerprint="rs_crs_rel_datasource",
        layer_id_snapshot="lyr_rs_crs_rel",
        layer_name_snapshot="reseau",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": fid}),
        operation_type=operation,
        attributes_json=attrs_json,
        geometry_wkb=geom_wkb,
        geometry_type="Point",
        crs_authid=crs,
        field_schema_json=json.dumps(fields or _FIELDS),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 9, 20, 9, 0, 0,
                            tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=CURRENT_SCHEMA_VERSION,
        new_geometry_wkb=new_geom_wkb,
        invalidated_at=None,
    )


def _witness_fids(spec) -> list:
    return [3 + i for i in range(len(spec["c"]))]


def _build(tmpdir: str, case: str, path_kind: str, event_base: int) -> dict:
    """Capture reelle sur un vrai fichier, puis changement de SCR."""
    from qgis.core import QgsCoordinateReferenceSystem, QgsGeometry, QgsPointXY
    from recoverland.core.geometry_utils import (
        extract_crs_authid, extract_geometry_wkb,
    )
    from recoverland.core.serialization import (
        build_full_snapshot, compute_update_delta,
    )

    spec = _CASES[case]
    path = str(Path(tmpdir) / f"{case}_{path_kind}.gpkg")
    rows = []
    if spec["a"]:
        rows.append((1, "A_AVANT", _wkb_point(*spec["a"][0])))
    if spec["b"]:
        rows.append((2, "B", _wkb_point(*spec["b"])))
    for fid, xy in zip(_witness_fids(spec), spec["c"]):
        rows.append((fid, f"C{fid}", _wkb_point(*xy)))
    if spec.get("d"):
        rows.append((6, "D_AVANT", _wkb_point(*spec["d"][0])))
    _write_gpkg(path, spec["file_epsg"], rows)

    layer = _open(path)
    if spec["capture_crs"]:
        layer.setCrs(QgsCoordinateReferenceSystem(spec["capture_crs"]))
    # Ce que edit_tracker range dans event.crs_authid: le SCR DECLARE.
    capture_crs = extract_crs_authid(layer)
    provider = layer.dataProvider()
    nom_idx = layer.fields().indexFromName("nom")
    events = {}
    if spec["a"]:
        old_wkb = extract_geometry_wkb(_feature(layer, 1))
        provider.changeGeometryValues(
            {1: QgsGeometry.fromPointXY(QgsPointXY(*spec["a"][1]))})
        provider.changeAttributeValues({1: {nom_idx: "A"}})
        new_wkb = extract_geometry_wkb(_feature(layer, 1))
        events["upd"] = _event(
            "UPDATE", 1, old_wkb, new_wkb, capture_crs,
            compute_update_delta({"nom": "A_AVANT"}, {"nom": "A"}, ["nom"]),
            event_base + 1)
    if spec["b"]:
        b_wkb = extract_geometry_wkb(_feature(layer, 2))
        provider.deleteFeatures([2])
        events["del"] = _event(
            "DELETE", 2, b_wkb, None, capture_crs,
            build_full_snapshot({"nom": "B"}), event_base + 2)
    if spec.get("d"):
        d_old = extract_geometry_wkb(_feature(layer, 6))
        provider.changeGeometryValues(
            {6: QgsGeometry.fromPointXY(QgsPointXY(*spec["d"][1]))})
        provider.changeAttributeValues({6: {nom_idx: "D"}})
        d_new = extract_geometry_wkb(_feature(layer, 6))
        events["upd2"] = _event(
            "UPDATE", 6, d_old, d_new, capture_crs,
            compute_update_delta({"nom": "D_AVANT"}, {"nom": "D"}, ["nom"]),
            event_base + 3)

    kind, new_epsg = spec["change"]
    if kind == "setcrs":
        layer.setCrs(QgsCoordinateReferenceSystem(
            new_epsg if isinstance(new_epsg, str) else f"EPSG:{new_epsg}"))
        if spec.get("forge_d") == "reprojected":
            # D alone now carries the REPROJECTED reading of its NEW
            # geometry: a witness of the opposite story.
            forged = _candidates(capture_crs, layer.crs().authid(),
                                 spec["d"][1])["reprojected"]
            provider.changeGeometryValues(
                {6: QgsGeometry.fromPointXY(QgsPointXY(*forged))})
    else:
        provider = None
        holder, layer = [layer], None
        layer = _rewrite(holder, path, new_epsg, kind == "reproject",
                         spec.get("after_extra", []))
    return {"layer": layer, "path": path, "events": events,
            "capture_crs": capture_crs, "layer_crs": layer.crs().authid()}


def _build_insert_undo(tmpdir: str, case: str, path_kind: str,
                       event_id: int) -> dict:
    """INSERT capture en EPSG:2154, puis fichier reprojete en EPSG:3857."""
    from recoverland.core.geometry_utils import (
        extract_crs_authid, extract_geometry_wkb,
    )
    from recoverland.core.serialization import build_full_snapshot

    spec = _INSERT_UNDO[case]
    path = str(Path(tmpdir) / f"{case}_{path_kind}.gpkg")
    rows = [(fid, values, _wkb_point(*xy)) for fid, values, xy in _IU_KEEP]
    x_fid, x_values, x_xy = _IU_X
    rows.append((x_fid, x_values, _wkb_point(*x_xy)))
    _write_gpkg(path, 2154, rows, _IU_NAMES)
    layer = _open(path)
    capture_crs = extract_crs_authid(layer)
    event = _event(
        "INSERT", x_fid, extract_geometry_wkb(_feature(layer, x_fid)), None,
        capture_crs, build_full_snapshot(dict(zip(_IU_NAMES, x_values))),
        event_id, fields=_IU_FIELDS)
    if not spec["x_still_there"]:
        layer.dataProvider().deleteFeatures([x_fid])
    holder, layer = [layer], None
    layer = _rewrite(holder, path, 3857, True, names=_IU_NAMES)
    return {"layer": layer, "path": path, "event": event,
            "capture_crs": capture_crs, "layer_crs": layer.crs().authid()}


def _xy(layer, fid):
    feat = _feature(layer, fid)
    if feat is None:
        return None
    geom = feat.geometry()
    if geom is None or geom.isNull() or geom.isEmpty():
        return None
    pt = geom.asPoint()
    return (pt.x(), pt.y())


def _fid_by_name(layer, nom):
    for feat in layer.dataProvider().getFeatures():
        if feat["nom"] == nom:
            return feat.id()
    return None


def _candidates(capture_crs, layer_crs, xy):
    """Position brute et position reprojetee de *xy*, dans le SCR couche."""
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsPointXY,
        QgsProject,
    )
    try:
        xform = QgsCoordinateTransform(
            QgsCoordinateReferenceSystem(capture_crs),
            QgsCoordinateReferenceSystem(layer_crs), QgsProject.instance())
        pt = xform.transform(QgsPointXY(*xy))
        repro = (pt.x(), pt.y())
        if not all(math.isfinite(v) for v in repro):
            repro = None
    except Exception:  # noqa: BLE001 - QgsCsException: hors domaine
        repro = None
    return {"raw": tuple(xy), "reprojected": repro}


def _tol(layer_crs) -> float:
    from qgis.core import QgsCoordinateReferenceSystem
    geographic = QgsCoordinateReferenceSystem(layer_crs).isGeographic()
    return 1e-7 if geographic else 1e-2


def _near(a, b, tol) -> bool:
    return (a is not None and b is not None
            and abs(a[0] - b[0]) <= tol and abs(a[1] - b[1]) <= tol)


def _classify(result, pos, cands, untouched, tol) -> str:
    if not result.get("success"):
        return "refused" if untouched else "refused_but_written"
    if pos is None:
        return "success_without_feature"
    if _near(pos, cands["raw"], tol):
        return "raw"
    if _near(pos, cands["reprojected"], tol):
        return "reprojected"
    return "elsewhere"


def _short(result) -> dict:
    return {"success": result.get("success"),
            "skipped": result.get("skipped"),
            "reason_code": result.get("reason_code"),
            "identity_level": result.get("identity_level"),
            "message": str(result.get("message", ""))[:200]}


def _play(ctx, case: str, path_kind: str, built: dict) -> dict:
    from recoverland.core.restore_executor import _apply_via_buffer
    from recoverland.core.restore_service import (
        restore_deleted_feature, restore_updated_feature,
    )

    spec = _CASES[case]
    layer = built["layer"]
    events = built["events"]
    capture_crs = built["capture_crs"]
    layer_crs = built["layer_crs"]
    tol = _tol(layer_crs)
    before_count = layer.dataProvider().featureCount()
    a_before = _xy(layer, 1) if spec["a"] else None
    c_before = {fid: _xy(layer, fid) for fid in _witness_fids(spec)}
    extra_before = {fid: _xy(layer, fid)
                    for fid, _nom, _xy_ in spec.get("after_extra", [])}
    # Le meme ordre pour les deux moteurs; "del_first" est celui du rewind
    # (phase 0: les suppressions sont restaurees avant les modifications).
    steps = (spec["order"] if isinstance(spec["order"], tuple)
             else ("del", "upd") if spec["order"] == "del_first"
             else ("upd", "del"))
    d_before = _xy(layer, 6) if spec.get("d") else None

    results = {}
    out = {}
    if path_kind == "svc":
        for kind in steps:
            if kind not in events:
                continue
            if kind in ("upd", "upd2"):
                results[kind] = dict(restore_updated_feature(
                    layer, events[kind]))
                layer.reload()
                continue
            results["del"] = dict(restore_deleted_feature(layer, events["del"]))
            layer.reload()
            # Rejeu immediat (double clic, relance apres un echec partiel):
            # la recherche de doublon doit prendre la MEME decision de SCR
            # que l'ecriture, sinon elle ne reconnait pas l'entite qu'elle
            # vient d'ecrire et en cree une seconde.
            count = layer.dataProvider().featureCount()
            replay = dict(restore_deleted_feature(layer, events["del"]))
            layer.reload()
            out["replay"] = {"result": _short(replay),
                             "skipped": bool(replay.get("skipped")),
                             "count_before": count,
                             "count_after": layer.dataProvider().featureCount()}
    else:
        remap = {}
        layer.startEditing()
        layer.beginEditCommand(SCENARIO_ID)
        for kind in steps:
            if kind not in events:
                continue
            comp = "INSERT" if kind == "del" else "UPDATE"
            results[kind] = dict(_apply_via_buffer(
                layer, comp, events[kind], remap, trace_id=ctx.trace_id))
        layer.endEditCommand()
        if not layer.commitChanges():
            results["commit_errors"] = list(layer.commitErrors())[:3]
            layer.rollBack()
        layer.reload()

    out.update({"results": {k: _short(v) if isinstance(v, dict) else v
                            for k, v in results.items()},
                "capture_crs": capture_crs, "layer_crs": layer_crs,
                "count_before": before_count,
                "count_after": layer.dataProvider().featureCount()})
    if out.get("replay"):
        # The replay may only add what the first restore added: counted
        # apart, the refusal checks below keep measuring the first write.
        out["count_after"] -= (out["replay"]["count_after"]
                               - out["replay"]["count_before"])
    if "upd" in events:
        pos = _xy(layer, 1)
        cands = _candidates(capture_crs, layer_crs, spec["a"][0])
        out["upd"] = {
            "pos": pos, "before": a_before, "cands": cands,
            "outcome": _classify(results["upd"], pos, cands,
                                 _near(pos, a_before, tol), tol),
        }
    if "upd2" in events:
        pos = _xy(layer, 6)
        cands = _candidates(capture_crs, layer_crs, spec["d"][0])
        out["upd2"] = {
            "pos": pos, "before": d_before, "cands": cands,
            "outcome": _classify(results["upd2"], pos, cands,
                                 _near(pos, d_before, tol), tol),
        }
    if "del" in events:
        fid = _fid_by_name(layer, "B")
        pos = _xy(layer, fid) if fid is not None else None
        cands = _candidates(capture_crs, layer_crs, spec["b"])
        out["del"] = {
            "pos": pos, "fid": fid, "cands": cands,
            "outcome": _classify(results["del"], pos, cands, fid is None, tol),
        }
    if c_before:
        out["c_moved"] = [fid for fid, xy in c_before.items()
                          if not _near(_xy(layer, fid), xy, tol)]
    if extra_before:
        out["extra_moved"] = [fid for fid, xy in extra_before.items()
                              if not _near(_xy(layer, fid), xy, tol)]
    return out


def _play_insert_undo(ctx, case: str, path_kind: str, built: dict) -> dict:
    from recoverland.core.restore_executor import _apply_via_buffer
    from recoverland.core.restore_service import restore_inserted_feature

    layer = built["layer"]
    before = sorted(f.id() for f in layer.dataProvider().getFeatures())
    y_before = _xy(layer, _IU_Y)
    if path_kind == "svc":
        result = dict(restore_inserted_feature(layer, built["event"]))
    else:
        layer.startEditing()
        result = dict(_apply_via_buffer(layer, "DELETE", built["event"], {},
                                        trace_id=ctx.trace_id))
        if not layer.commitChanges():
            result["commit_errors"] = list(layer.commitErrors())[:3]
            layer.rollBack()
    layer.reload()
    after = sorted(f.id() for f in layer.dataProvider().getFeatures())
    return {"result": _short(result), "before": before, "after": after,
            "y_before": y_before, "y_after": _xy(layer, _IU_Y),
            "capture_crs": built["capture_crs"],
            "layer_crs": built["layer_crs"]}


def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_rs_crs_rel_")
    ctx.data["tmpdir"] = tmpdir
    built = {}
    base = 7000
    for case in _CASES:
        for path_kind in _PATHS:
            base += 10
            built[(case, path_kind)] = _build(tmpdir, case, path_kind, base)
    for case in _INSERT_UNDO:
        for path_kind in _PATHS:
            base += 10
            built[(case, path_kind)] = _build_insert_undo(
                tmpdir, case, path_kind, base)
    ctx.data["built"] = built
    ctx.data["fixture"] = {
        f"{case}/{kind}": {"capture_crs": b["capture_crs"],
                           "layer_crs": b["layer_crs"],
                           "events": sorted(b.get("events") or ["insert"])}
        for (case, kind), b in built.items()}
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id} tmpdir={tmpdir} "
         f"fixture={ctx.data['fixture']}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"{SCENARIO_ID} run start: trace_id={ctx.trace_id}", "INFO")
    played = {}
    for (case, path_kind), built in ctx.data["built"].items():
        play = _play_insert_undo if case in _INSERT_UNDO else _play
        try:
            played[(case, path_kind)] = play(ctx, case, path_kind, built)
        except Exception as exc:  # noqa: BLE001 - reported as an assertion
            played[(case, path_kind)] = {"crash": f"{type(exc).__name__}: {exc}"}
        flog(f"{SCENARIO_ID} {case}/{path_kind}: trace_id={ctx.trace_id} "
             f"{played[(case, path_kind)]}", "INFO")
    ctx.data["played"] = played

    for built in ctx.data["built"].values():
        _release(built["layer"])
        built["layer"] = None
    gc.collect()
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)
    flog(f"{SCENARIO_ID} run end: trace_id={ctx.trace_id}", "INFO")


def _km(a, b):
    if a is None or b is None:
        return None
    return round(math.hypot(a[0] - b[0], a[1] - b[1]) / 1000.0, 1)


def _insert_undo_assertions(played) -> list:
    out = []
    for case, spec in _INSERT_UNDO.items():
        for path_kind in _PATHS:
            rec = played.get((case, path_kind)) or {}
            label = _PATH_LABEL[path_kind]
            if "crash" in rec or not rec:
                out.append((f"{case}_{path_kind}_ran", False,
                            f"{label}: le cas n'a pas pu etre joue: "
                            f"{rec.get('crash', 'absent')}"))
                continue
            res = rec["result"]
            gone = sorted(set(rec["before"]) - set(rec["after"]))
            y_kept = (_IU_Y in rec["after"]
                      and rec["y_after"] == rec["y_before"])
            if spec["x_still_there"]:
                ok = res["success"] is True and gone == [_IU_X[0]] and y_kept
                want = (f"l'entite inseree (fid {_IU_X[0]}, encore dans la "
                        f"couche, reprojetee avec le fichier) supprimee, et "
                        f"elle seule")
            else:
                ok = (res["success"] is False and not gone and y_kept
                      and bool(res.get("reason_code")))
                want = ("refus nomme, rien de supprime: l'entite inseree n'est "
                        "plus la et l'histoire du SCR est indecise; l'entite "
                        f"{_IU_Y} ('borne', 'LYON', a 3 km) est une autre "
                        "entite")
            out.append((
                f"{case}_{path_kind}_deletes_only_the_inserted_feature",
                ok,
                f"{label}, annulation d'un INSERT (une SUPPRESSION) sur un "
                f"fichier Lambert-93 reecrit en EPSG:3857: attendu {want}. "
                f"Supprimees: {gone}; Y gardee={y_kept}; result={res}. "
                f"Pour l'utilisateur, un echec ici veut dire: une entite sans "
                f"rapport effacee de sa couche, annoncee comme une annulation "
                f"reussie.",
            ))
    return out


def assertions(ctx):
    from scripts.validation.assert_log import (
        assert_log_absent, assert_log_contains,
    )

    out = []
    fixture = ctx.data.get("fixture") or {}
    played = ctx.data.get("played") or {}

    # ===== Sanity: le decor est bien celui qu'on croit ===================
    expected_crs = dict(_EXPECTED_CRS)
    expected_crs.update({case: ("EPSG:2154", "EPSG:3857")
                         for case in _INSERT_UNDO})
    bad = {key: (f["capture_crs"], f["layer_crs"])
           for key, f in fixture.items()
           if (f["capture_crs"], f["layer_crs"])
           != expected_crs[key.split("/")[0]]}
    out.append((
        "fixture_crs_before_and_after",
        len(fixture) == 2 * (len(_CASES) + len(_INSERT_UNDO)) and not bad,
        f"SCR capture/couche inattendus: {bad} (fixture={len(fixture)} "
        f"copies): sans le bon decor, le scenario ne prouve rien",
    ))
    # Les deux hypotheses doivent etre eloignees: sinon "brut" et
    # "reprojete" ne se distinguent pas a la relecture.
    close = []
    for key, rec in played.items():
        if key[0] in _INSERT_UNDO or _CASES[key[0]].get("same_position"):
            continue
        for kind in ("upd", "upd2", "del"):
            cands = (rec.get(kind) or {}).get("cands")
            if not cands or cands["reprojected"] is None:
                continue
            gap = math.hypot(cands["raw"][0] - cands["reprojected"][0],
                             cands["raw"][1] - cands["reprojected"][1])
            if gap < 1.0:
                close.append((key, kind, gap))
    out.append((
        "fixture_hypotheses_are_far_apart",
        not close,
        f"brut et reprojete trop proches pour etre distingues: {close}",
    ))

    # ===== Chaque cas, chaque moteur =====================================
    for case, spec in _CASES.items():
        for path_kind in _PATHS:
            rec = played.get((case, path_kind)) or {}
            label = _PATH_LABEL[path_kind]
            if "crash" in rec or not rec:
                out.append((
                    f"{case}_{path_kind}_ran", False,
                    f"{label}: le cas n'a pas pu etre joue: "
                    f"{rec.get('crash', 'absent')}"))
                continue
            for kind, want in spec["expect"].items():
                got = (rec.get(kind) or {}).get("outcome")
                pos = (rec.get(kind) or {}).get("pos")
                cands = (rec.get(kind) or {}).get("cands") or {}
                target = cands.get(want) if want in ("raw", "reprojected") \
                    else None
                what = {"upd": "UPDATE (entite deplacee)",
                        "upd2": "UPDATE (seconde entite deplacee)",
                        "del": "DELETE (entite recreee)"}[kind]
                wrong = cands.get("raw" if want == "reprojected"
                                  else "reprojected")
                out.append((
                    f"{case}_{path_kind}_{kind}_is_{want}",
                    got == want,
                    f"{spec['story']}: {label}, {what}: attendu {want}, "
                    f"obtenu {got}. Position relue {pos}, brute "
                    f"{cands.get('raw')}, reprojetee "
                    f"{cands.get('reprojected')} (ecart a la bonne position: "
                    f"{_km(pos, target)} km; a l'autre lecture: "
                    f"{_km(pos, wrong)} km). Un {got!r} ici veut dire, pour "
                    f"l'utilisateur: "
                    + ("entite deplacee alors que la restauration annonce "
                       "un succes" if got in ("raw", "reprojected", "elsewhere")
                       else "restauration refusee alors que les preuves "
                       "designaient une seule position" if got == "refused"
                       else "etat incoherent")
                    + f". result={rec['results'].get(kind)}",
                ))
                if want == "refused":
                    res = rec["results"].get(kind) or {}
                    msg = res.get("message") or ""
                    capture_crs, layer_crs = expected_crs[case]
                    out.append((
                        f"{case}_{path_kind}_{kind}_refusal_is_named",
                        res.get("reason_code") == "crs_ambiguous"
                        and capture_crs in msg and layer_crs in msg,
                        f"{label}: le refus doit dire pourquoi, precisement "
                        f"(reason_code=crs_ambiguous, les deux SCR nommes), "
                        f"pas un echec generique: result={res}",
                    ))
                    out.append((
                        f"{case}_{path_kind}_{kind}_refusal_writes_nothing",
                        rec.get("count_after") == rec.get("count_before"),
                        f"{label}: un refus ne doit rien ecrire: "
                        f"{rec.get('count_before')} entites avant, "
                        f"{rec.get('count_after')} apres",
                    ))
            if spec.get("after_extra"):
                out.append((
                    f"{case}_{path_kind}_stray_untouched",
                    rec.get("extra_moved") == [],
                    f"{label}: le point parasite n'etait vise par aucun "
                    f"evenement et a bouge: {rec.get('extra_moved')}",
                ))
            if spec["c"]:
                out.append((
                    f"{case}_{path_kind}_witness_untouched",
                    rec.get("c_moved") == [],
                    f"{label}: les entites temoins n'etaient visees par "
                    f"aucun evenement et ont bouge: {rec.get('c_moved')}",
                ))

    # ===== Rejeu: la recherche de doublon suit la meme decision ==========
    for case, spec in _CASES.items():
        if "del" not in spec["expect"]:
            continue
        replay = (played.get((case, "svc")) or {}).get("replay") or {}
        refused = spec["expect"]["del"] == "refused"
        out.append((
            f"{case}_svc_replay_is_idempotent",
            bool(replay)
            and replay.get("count_after") == replay.get("count_before")
            and (replay["result"].get("success") is False if refused
                 else replay.get("skipped") is True),
            f"{spec['story']}: relancer la meme restauration doit "
            + ("refuser encore, sans rien ecrire" if refused else
               "la reconnaitre comme deja faite (skip), pas creer un "
               "doublon")
            + f": {replay}",
        ))

    # ===== Les deux moteurs prennent la meme decision ====================
    diverge = []
    for case, spec in _CASES.items():
        for kind in spec["expect"]:
            svc = ((played.get((case, "svc")) or {}).get(kind) or {}).get(
                "outcome")
            rw = ((played.get((case, "rw")) or {}).get(kind) or {}).get(
                "outcome")
            if svc != rw:
                diverge.append((case, kind, svc, rw))
    out.append((
        "both_engines_take_the_same_decision",
        not diverge,
        f"la restauration unitaire et le rewind decident differemment pour "
        f"la meme geometrie: {diverge}. L'utilisateur obtiendrait deux "
        f"positions selon le bouton utilise.",
    ))

    # ===== Annulation d'un INSERT sous une histoire indecise =============
    out.extend(_insert_undo_assertions(played))

    # ===== Chaque decision est journalisee ===============================
    for decision, src, dst in (
            ("relabel", "EPSG:4326", "EPSG:2154"),
            ("relabel", "EPSG:3946", "EPSG:2154"),
            ("reprojection", "EPSG:4326", "EPSG:2154"),
            ("ambiguous", "EPSG:3857", "EPSG:4326"),
            ("relabel", "EPSG:3857", "EPSG:4326"),
            ("reprojection", "EPSG:4326", "EPSG:3857"),
            ("ambiguous", "EPSG:4326", "EPSG:27700"),
            ("ambiguous", "EPSG:2154", "EPSG:3857"),
            ("reprojection", "EPSG:2154", "EPSG:3857"),
            ("ambiguous", "EPSG:3857", "EPSG:2154"),
            ("relabel", "EPSG:3857", "EPSG:2154")):
        out.append(assert_log_contains(
            ctx.records,
            rf"crs_decision decision={decision} src_crs={src} "
            rf"layer_crs={dst} evidence=",
            name=f"decision_logged_{decision}_{src}_{dst}".replace(":", ""),
            min_count=1,
        ))
    # Hypothesis revised (v7): this check read
    # "evidence=area_and_extent_disagree", i.e. the data extent contradicting
    # a weak area verdict. The extent is no longer evidence (being near the
    # other features says nothing of the story), so a weak area verdict is
    # refused unless something that IS evidence corroborates it. The check
    # keeps its strength: the refusal of this weak verdict must be named in
    # the log, with the reason it was not trusted.
    out.append(assert_log_contains(
        ctx.records,
        r"crs_decision decision=ambiguous src_crs=EPSG:4326 "
        r"layer_crs=EPSG:27700 evidence=weak_area_uncorroborated",
        name="conflicting_evidence_is_named",
        min_count=1,
    ))
    # Un point parasite isole ne doit pas opposer son veto a une zone de
    # validite categorique: les degres bruts sont a des millions de metres
    # de la France metropolitaine.
    # Hypothesis revised (v7): the pattern also required
    # "raw_in_extent=True repro_in_extent=False", the proof that the extent
    # pointed the other way and was overruled. The extent is no longer
    # consulted at all, so no feature can point anywhere; what must hold is
    # that the decision rests on the categorical area verdict (below) and
    # that the stray feature stays untouched and B lands reprojected (the
    # case assertions above).
    out.append(assert_log_contains(
        ctx.records,
        r"crs_decision decision=reprojection src_crs=EPSG:4326 "
        r"layer_crs=EPSG:2154 evidence=\S*area_strong.*area_verdict=strong",
        name="strong_area_verdict_outweighs_a_stray_feature",
        min_count=1,
    ))
    # Deux identifiants pour une meme definition: rien a decider, et
    # surtout pas un refus "ambigu".
    out.append(assert_log_contains(
        ctx.records,
        r"crs_decision decision=equivalent src_crs=EPSG:4326 "
        r"layer_crs=OGC:CRS84 evidence=same_position",
        name="equivalent_ids_are_not_a_decision",
        min_count=1,
    ))
    # Le rewind porte le trace_id jusqu'a la decision.
    out.append(assert_log_contains(
        ctx.records,
        rf"crs_decision decision=\S+ .*trace_id={ctx.trace_id}",
        name="rewind_decision_carries_trace_id",
        min_count=1,
    ))
    # Tous les cas EPSG:2154 -> EPSG:3857 sont des reprojections en place:
    # aucune decision "relabel" ne doit apparaitre pour cette paire, que ce
    # soit pour une preuve d'identite ou pour une ecriture.
    out.append(assert_log_absent(
        ctx.records,
        r"crs_decision decision=relabel src_crs=EPSG:2154 "
        r"layer_crs=EPSG:3857",
        name="no_relabel_decided_for_an_in_place_reprojection",
    ))
    # L'histoire vient d'un temoin (la cible de l'UPDATE porte la lecture
    # reprojetee de sa NOUVELLE geometrie), pas de la proximite d'autres
    # entites.
    out.append(assert_log_contains(
        ctx.records,
        r"crs_decision decision=reprojection src_crs=EPSG:2154 "
        r"layer_crs=EPSG:3857 evidence=\S*witness",
        name="reprojection_story_rests_on_a_witness",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"crs_decision decision=relabel src_crs=EPSG:3857 "
        r"layer_crs=EPSG:2154 evidence=\S*witness",
        name="relabel_story_rests_on_a_witness",
        min_count=1,
    ))
    # Une geometrie sans preuve propre suit l'histoire deja etablie pour
    # la couche (et le dit), elle ne la redecide pas.
    out.append(assert_log_contains(
        ctx.records,
        r"crs_decision decision=(relabel|reprojection) src_crs=\S+ "
        r"layer_crs=\S+ evidence=story ",
        name="geometry_without_evidence_follows_the_story",
        min_count=1,
    ))

    # Une preuve qui contredit l'histoire deja etablie la rend
    # conflictuelle: la geometrie qui la contredit et toutes les suivantes
    # sont refusees, et le conflit est nomme.
    out.append(assert_log_contains(
        ctx.records,
        r"crs_story conflict src_crs=EPSG:3946 .*settled=relabel\(\S*witness"
        r"\S*\) contradicted_by=reprojection\(\S*witness",
        name="story_conflict_is_named",
        min_count=1,
    ))
    for evidence in ("contradicts_story", "story_conflict"):
        out.append(assert_log_contains(
            ctx.records,
            rf"crs_decision decision=ambiguous src_crs=EPSG:3946 "
            rf"layer_crs=EPSG:2154 evidence={evidence} ",
            name=f"story_conflict_refuses_{evidence}",
            min_count=1,
        ))

    # ===== Propagation du trace_id ======================================
    out.append(assert_log_contains(
        ctx.records,
        rf"{SCENARIO_ID}.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))
    return out


def teardown(ctx):
    for built in (ctx.data.get("built") or {}).values():
        _release(built.get("layer"))
        built["layer"] = None
    gc.collect()
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
