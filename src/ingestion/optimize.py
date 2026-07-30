def optimize_source(cfg, env, spark):
    """Compact a source's Bronze table. Config-driven, zero source-specific
    code: every source gets compaction for free (platform thesis)."""
    table = env.delta_table(cfg, spark)  # path local / UC name cloud; el seam lo absorbe

    # DESCRIBE DETAIL lee el tx log, no los datos: barato y exacto.
    # numFiles ES el numero del small-files tax.
    before = table.detail().select("numFiles", "sizeInBytes").first()

    builder = table.optimize()
    zorder = getattr(cfg, "zorder_by", None)  # ajusta al accessor real de tu SourceConfig
    if zorder:
        # Z-order co-localiza filas con el mismo valor de columna en los mismos
        # archivos, para que un read filtrado brinque archivos enteros. Off por
        # default: es una decision de read-shape del source, no un default de compaction.
        result = builder.executeZOrderBy(*zorder)
    else:
        # Bin-packing: fusiona archivos pequenos en archivos de ~1 GB. Es el paso
        # de compaction LSM (DDIA Ch 3): appendeas segments chicos baratos en el
        # write, los consolidas despues para bajar read amplification.
        result = builder.executeCompaction()

    after = table.detail().select("numFiles", "sizeInBytes").first()
    return before, after, result