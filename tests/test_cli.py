from gridlake.cli import main


def test_cli_commands(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("GRIDLAKE_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    assert main(["run", "--end", "2026-10-04", "--days", "3", "--telemetry-hours", "1"]) == 0
    assert "OK" in capsys.readouterr().out
    assert main(["sql", "SELECT count(*) AS n FROM gold_ba_daily_energy"]) == 0
    assert "24" in capsys.readouterr().out
    assert main(["quality", "--samples", "1"]) == 0
    assert "quarantined rows" in capsys.readouterr().out
    assert main(["lineage"]) == 0
    assert "gold.ba_daily_energy" in capsys.readouterr().out
    question = "What was ERCOT's carbon intensity on 2026-10-02?"
    assert main(["ask", question, "--context-only", "--json"]) == 0
    assert "ba_daily_energy/ERCO/2026-10-02" in capsys.readouterr().out
    assert main(["ask", question, "--trace"]) == 0
    out = capsys.readouterr().out
    assert "> run_sql(" in out and "ERCO carbon intensity on 2026-10-02:" in out
    assert "tables=gold.ba_daily_energy" in out
    assert (
        main(["step", "gold", "--end", "2026-10-04", "--since", "2026-01-01T00:00:00+00:00"]) == 0
    )
