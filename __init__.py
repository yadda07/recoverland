def classFactory(iface):
    from .recover import RecoverPlugin
    return RecoverPlugin(iface)
