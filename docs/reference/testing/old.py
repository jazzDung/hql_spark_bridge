def _analyze_statement(self, stmt: exp.Expression, target_table: str) -> tuple[str, bool, Optional[str]]:
    """
    Trả về: (Source ID, Có phải là DDL bảng tạm không, Tên bảng tạm nếu có)
    """
    tables = list(stmt.find_all(exp.Table))

    # 1. Bắt các lệnh DDL tạo bảng tạm
    if isinstance(stmt, (exp.Create, exp.Drop, exp.TruncateTable)):
        for t in tables:
            if t.name.upper().startswith('TEMP_'):
                return "COMMON_INIT", True, t.name.upper()

    # 2. Các luật bóc tách Source ID (như cũ)
    partition = stmt.find(exp.Partition)
    if partition:
        for eq in partition.find_all(exp.EQ):
            if isinstance(eq.left, exp.Column) and eq.left.name.upper() == 'SOURCE_KEY':
                if isinstance(eq.right, exp.Literal):
                    return eq.right.name.upper(), False, None

    if isinstance(stmt, exp.Insert):
        select = stmt.expression

        if isinstance(select, exp.Select):
            for alias in select.find_all(exp.Alias):
                if alias.alias.upper() == 'SOURCE_NAME':
                    if isinstance(alias.this, exp.Literal):
                        return alias.this.name.upper(), False, None

    if stmt.find(exp.With) is not None:
        cte = stmt.find(exp.With).expressions[0].this
        if isinstance(cte, exp.Select):
            for alias in cte.find_all(exp.Alias):
                if alias.alias.upper() == 'SOURCE_NAME':
                    if isinstance(alias.this, exp.Literal):
                        return alias.this.name.upper(), False, None

            for table in cte.find_all(exp.Table):
                return table.name, False, None

    source_candidates = set()
    for t in tables:
        name = t.name.upper()
        parts = name.split('_')
        if len(parts) >= 2 and parts[0] in ('T', 'M', 'R'):
            source_candidates.add(parts[1])

    if len(source_candidates) == 1:
        return list(source_candidates)[0], False, None
    elif len(source_candidates) > 1:
        return "FINAL_CONSOLIDATION", False, None

    for t in tables:
        if t.name.upper() == target_table:
            return "FINAL_CONSOLIDATION", False, None

    return "UNKNOWN_SOURCE", False, None
