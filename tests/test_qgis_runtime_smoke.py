import pytest

pytest.importorskip("pytest_qgis")


def test_qgis_runtime_imports(qgis_app):
    from recoverland.recover import RecoverPlugin
    from recoverland.recover_dialog import RecoverDialog
    from recoverland.core.journal_manager import JournalManager

    assert RecoverPlugin is not None
    assert RecoverDialog is not None
    assert JournalManager is not None
