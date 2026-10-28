# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Contrôle et ajout, par patch Git, de la signature personnelle des sources.

Python 3.9+, bibliothèque standard uniquement. Copier ce script à la racine
du plugin. Contrôle : python verifier_signature.py ; ajout : --appliquer.
Les fichiers tiers et les sources générées ne doivent jamais être réattribués.
"""

import argparse
import codecs
import configparser
import difflib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess  # nosec B404
import sys
import tokenize


MARQUEUR = "Youcef ADDA | GEODECI"
EXTENSIONS = {".py", ".pyw", ".js", ".cjs", ".mjs", ".ps1",
              ".sh", ".sql", ".bat", ".cmd", ".gs"}
DOSSIERS_EXCLUS = {
    ".git", ".hg", ".svn", "__pycache__", ".pytest_cache", ".mypy_cache",
    ".ruff_cache", "cache", "caches", "archive", "archives", "backup",
    "backups", "livraison", "livraisons", "output", "outputs", "node_modules",
    "vendor", "_vendor", "vendored", "third_party", "thirdparty", "external",
    "venv", ".venv", "dist", "build", "site-packages", "plugins",
    "old", "ancienne_version", "anciennes_versions", "zone de test",
    "libredwg-master", "libredwg", "third-party", "3rdparty",
}
LIGNES = (
    MARQUEUR,
    "FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; "
    "audit, automatisation et accompagnement.",
    "EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; "
    "audits, automation and business support.",
    "AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، "
    "والتدقيق والأتمتة ومرافقة المؤسسات.",
    "Site / Website / الموقع : https://geodeci.xyz/",
    "Contact professionnel / Business enquiries / للتواصل المهني : "
    "youcef.geodesien@gmail.com | contact@geodeci.xyz",
    "Détails / Details / التفاصيل : SIGNATURE.md",
    "============================================================================",
)
GENERES = re.compile(
    r"pyrcc[456]?|pyuic[456]?|resource object code|form implementation generated|"
    r"automatically generated|auto[- ]generated|do not edit.*generated|"
    r"generated (?:by|from).*(?:protoc|protobuf|qt designer|resource compiler)", re.I)
AUTEUR = re.compile(
    r"(?im)^[ \t#*/;!-]*(?:__author__|author|auteur)[ \t]*[:=][ \t]*([^\r\n]+)")
PROPRIETAIRE = re.compile(r"youcef|youssef|\badda\b|\byadda\b|geodeci", re.I)
COLLECTIF = re.compile(r"\bnge\b|devteam|devtem|gisengine|st-air|[ée]quipe vtt", re.I)
COPYRIGHT = re.compile(
    r"(?im)^[ \t#*/;!-]*(?:__copyright__[ \t]*=[ \t]*[\"']?|copyright[ \t]*:?[ \t]*)"
    r"(?:\([cC]\)|©)?[ \t]*(?:\d{4}(?:[-, ]+\d{4})*[ \t]*)?(?:by[ \t]+)?([^\r\n]+)"
)


def encodage(contenu, extension):
    if extension in {".py", ".pyw"}:
        nom, _ = tokenize.detect_encoding(io.BytesIO(contenu).readline)
        texte = contenu.decode(nom)
        return ("utf-8" if nom == "utf-8-sig" else nom), texte
    if contenu.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        raise ValueError("UTF-16 : patch textuel non pris en charge ; fichier conservé")
    try:
        return "utf-8", contenu.decode("utf-8-sig")
    except UnicodeDecodeError:
        return "cp1252", contenu.decode("cp1252")


def raison_exclusion(chemin, texte):
    nom = chemin.name.lower()
    if nom.endswith((".min.js", ".min.cjs", ".min.mjs")):
        return "JavaScript minifié"
    if nom in {"resources.py", "resources_rc.py"} or nom.endswith("_rc.py"):
        return "ressource générée"
    debut = "\n".join(texte[:16384].splitlines()[:60])
    if nom == "verifier_signature.py" and debut.startswith("# " + MARQUEUR):
        return None
    if GENERES.search(debut):
        return "source générée"
    # Une attribution explicite à un autre auteur reste intacte. Les crédits
    # copyright et la licence ne sont jamais effacés, même pour un fichier signé.
    auteurs = AUTEUR.findall(debut)
    if auteurs and any(
        not (PROPRIETAIRE.search(a) or COLLECTIF.search(a)) for a in auteurs
    ):
        return "auteur tiers explicite"
    mentions = COPYRIGHT.findall(debut)
    if mentions and any(
        not (PROPRIETAIRE.search(a) or COLLECTIF.search(a)) for a in mentions
    ):
        return "copyright tiers explicite"
    return None


def lignes_binaires(contenu):
    morceaux = contenu.split(b"\n")
    return [ligne + b"\n" for ligne in morceaux[:-1]] + (
        [morceaux[-1]] if morceaux[-1] else []
    )


def position_insertion(contenu, extension):
    bom = len(codecs.BOM_UTF8) if contenu.startswith(codecs.BOM_UTF8) else 0
    lignes = lignes_binaires(contenu[bom:])
    nombre = 0
    if lignes and lignes[0].startswith(b"#!"):
        nombre = 1
    if extension in {".py", ".pyw"}:
        for numero, ligne in enumerate(lignes[:2]):
            if re.match(br"^[ \t\f]*#.*?coding[:=][ \t]*[-\w.]+", ligne):
                nombre = max(nombre, numero + 1)
    elif extension in {".bat", ".cmd"} and lignes:
        if lignes[0].strip().lower() == b"@echo off":
            nombre = 1 if lignes[0].endswith(b"\n") else 0
    elif extension in {".qml", ".js", ".cjs", ".mjs"} and lignes:
        if lignes[0].strip().startswith(b".pragma "):
            nombre = 1
    elif extension == ".ps1":
        while nombre < len(lignes) and lignes[nombre].lstrip().lower().startswith(b"#requires"):
            nombre += 1
    position = bom + sum(len(ligne) for ligne in lignes[:nombre])
    if position > bom and not contenu[:position].endswith(b"\n"):
        raise ValueError("directive initiale sans fin de ligne : fichier conservé")
    return position


def preparer(chemin, contenu, codec):
    extension = chemin.suffix.lower()
    prefixe = "--" if extension == ".sql" else (
        "@REM" if extension in {".bat", ".cmd"} else (
            "//" if extension in {".qml", ".js", ".cjs", ".mjs", ".gs"} else "#"))
    fin = "\r\n" if b"\r\n" in contenu else "\n"
    try:
        entete = (fin.join(prefixe + " " + ligne for ligne in LIGNES) + fin).encode(codec)
    except UnicodeEncodeError as erreur:
        raise ValueError("encodage incompatible avec l'arabe : fichier conservé") from erreur
    position = position_insertion(contenu, extension)
    attendu = contenu[:position] + entete + contenu[position:]
    # Seuls des commentaires sont insérés. Le corps original, y compris la
    # docstring et les imports futurs, est vérifié octet par octet après patch.
    return attendu, position, len(entete)


def chemin_git(prefixe, relatif):
    donnees = (prefixe + relatif).encode("utf-8")
    # Git attend les échappements octaux de ses chemins C-quoted.
    return b'"' + b"".join(
        bytes([valeur]) if 32 <= valeur < 127 and valeur not in (34, 92)
        else ("\\%03o" % valeur).encode("ascii") for valeur in donnees) + b'"'


def construire_patch(relatif, avant, apres):
    lignes = difflib.diff_bytes(
        difflib.unified_diff, lignes_binaires(avant), lignes_binaires(apres),
        fromfile=chemin_git("a/", relatif), tofile=chemin_git("b/", relatif), n=3)
    morceaux = []
    for ligne in lignes:
        morceaux.append(ligne)
        if not ligne.endswith(b"\n"):
            morceaux.append(b"\n\\ No newline at end of file\n")
    return b"".join(morceaux)


def analyser(racine, exclusions):
    rapport = {"racine": str(racine), "signes": 0, "manquants": 0,
               "exclus": 0, "appliques": 0, "fichiers_manquants": [],
               "fichiers_exclus": [], "erreurs": []}
    changements = []
    if (racine / "metadata.txt").is_file():
        try:
            config = configparser.ConfigParser(interpolation=None, strict=False)
            config.read(racine / "metadata.txt", encoding="utf-8-sig")
            general = config["general"]
            if "Youcef ADDA" not in general.get("author", ""):
                raise ValueError("metadata.txt : auteur Youcef ADDA absent")
            about = general.get("about", "")
            requis_about = (
                "GEODECI", "https://geodeci.xyz/",
                "youcef.geodesien@gmail.com", "contact@geodeci.xyz",
                "EN:", "AR:",
            )
            if any(texte not in about for texte in requis_about):
                raise ValueError("metadata.txt : présentation trilingue ou contact absent")
            signature = (racine / "SIGNATURE.md").read_text(encoding="utf-8-sig")
            requis_sig = (
                "Youcef ADDA", "GEODECI", "## Français", "## English",
                "## العربية", "contact@geodeci.xyz", "https://geodeci.xyz/",
            )
            if any(texte not in signature for texte in requis_sig):
                raise ValueError("SIGNATURE.md : signature trilingue incomplète")
        except (OSError, ValueError, KeyError, configparser.Error) as erreur:
            rapport["erreurs"].append(
                {"chemin": "metadata.txt / SIGNATURE.md", "erreur": str(erreur)}
            )
    for dossier, sous_dossiers, noms in os.walk(racine, followlinks=False):
        sous_dossiers[:] = sorted(nom for nom in sous_dossiers
                                 if nom.lower() not in DOSSIERS_EXCLUS
                                 and not nom.startswith(".")
                                 and not (Path(dossier) / nom).is_symlink())
        for nom in sorted(noms):
            chemin = Path(dossier) / nom
            if chemin.suffix.lower() not in EXTENSIONS:
                continue
            relatif = chemin.relative_to(racine).as_posix()
            if chemin.is_symlink() or relatif.casefold() in exclusions:
                rapport["exclus"] += 1
                rapport["fichiers_exclus"].append(
                    {"chemin": relatif, "raison": "exclusion explicite ou lien"}
                )
                continue
            try:
                contenu = chemin.read_bytes()
                codec, texte = encodage(contenu, chemin.suffix.lower())
                raison = raison_exclusion(chemin, texte)
                if raison:
                    rapport["exclus"] += 1
                    rapport["fichiers_exclus"].append({"chemin": relatif, "raison": raison})
                elif all(ligne in texte[:5000] for ligne in LIGNES[:-1]):
                    rapport["signes"] += 1
                elif MARQUEUR in texte[:5000]:
                    raise ValueError("signature partielle ou altérée : vérifier l'en-tête existant")
                else:
                    rapport["manquants"] += 1
                    rapport["fichiers_manquants"].append(relatif)
                    attendu, position, longueur = preparer(chemin, contenu, codec)
                    changements.append((
                        chemin, relatif, contenu, attendu,
                        position, longueur, chemin.stat().st_mode,
                    ))
            except (OSError, UnicodeError, SyntaxError, ValueError) as erreur:
                rapport["erreurs"].append({"chemin": relatif, "erreur": str(erreur)})
    return rapport, changements


def main():
    analyseur = argparse.ArgumentParser(description=__doc__)
    analyseur.add_argument("--racine", type=Path, default=Path(__file__).resolve().parent)
    groupe = analyseur.add_mutually_exclusive_group()
    groupe.add_argument("--appliquer", action="store_true", help="ajouter les signatures par git apply")
    groupe.add_argument(
        "--patch", action="store_true",
        help="écrire le patch sur stdout, le rapport JSON sur stderr",
    )
    analyseur.add_argument("--exclure", action="append", default=[], metavar="CHEMIN_RELATIF",
                           help="exclure un fichier exact ; option répétable")
    arguments = analyseur.parse_args()
    racine = arguments.racine.resolve()
    if not racine.is_dir():
        analyseur.error("la racine doit être un dossier existant")
    exclusions = {nom.replace("\\", "/").removeprefix("./").casefold()
                  for nom in arguments.exclure}
    rapport, changements = analyser(racine, exclusions)
    patch = b"".join(construire_patch(relatif, avant, apres)
                     for _, relatif, avant, apres, _, _, _ in changements)
    if arguments.patch:
        sys.stdout.buffer.write(patch)
    elif arguments.appliquer and changements and not rapport["erreurs"]:
        executable = shutil.which("git")
        if not executable:
            rapport["erreurs"].append(
                {"erreur": "Git introuvable. Utiliser --patch pour produire le patch."}
            )
        else:
            try:
                for chemin, _, avant, _, _, _, _ in changements:
                    if chemin.read_bytes() != avant:
                        raise ValueError("fichier modifié pendant l'analyse : " + str(chemin))
                for options in (["--check"], []):
                    cmd = [
                        executable, "-c", "core.autocrlf=false",
                        "-c", "core.safecrlf=false", "apply",
                        "--whitespace=nowarn", *options, "-",
                    ]
                    resultat = subprocess.run(  # nosec B603
                        cmd, cwd=str(racine), input=patch,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    )
                    if resultat.returncode:
                        raise ValueError(
                            resultat.stderr.decode("utf-8", errors="replace").strip()
                        )
                for chemin, relatif, avant, attendu, position, longueur, mode in changements:
                    actuel = chemin.read_bytes()
                    corps = actuel[:position] + actuel[position + longueur:]
                    digest_corps = hashlib.sha256(corps).digest()
                    digest_avant = hashlib.sha256(avant).digest()
                    if actuel != attendu or digest_corps != digest_avant:
                        raise ValueError("corps modifié après patch : " + relatif)
                    if chemin.stat().st_mode != mode:
                        raise ValueError("permissions modifiées après patch : " + relatif)
                appliques = len(changements)
                rapport, _ = analyser(racine, exclusions)
                rapport["appliques"] = appliques
            except (OSError, ValueError) as erreur:
                rapport["erreurs"].append({"erreur": str(erreur)})
    sortie = sys.stderr if arguments.patch else sys.stdout
    print(json.dumps(rapport, ensure_ascii=True, indent=2), file=sortie)
    return 1 if rapport["manquants"] or rapport["erreurs"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
