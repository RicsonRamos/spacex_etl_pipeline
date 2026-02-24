import pytest
from unittest.mock import patch, MagicMock
import subprocess
import os
from src.utils.dbt_tools import run_dbt


# Testes de sucesso

@patch("src.utils.dbt_tools.subprocess.run")
@patch("src.utils.dbt_tools.logger")
def test_run_dbt_all_commands_success(mock_logger, mock_run):
    """Testa execução bem-sucedida de todos os comandos dbt"""
    # Configura mock para sucesso
    mock_run.return_value = MagicMock(
        stdout="Success: OK\nActual: 10 rows",
        stderr="",
        returncode=0
    )
    
    run_dbt()
    
    # Verifica que todos os 4 comandos foram executados
    assert mock_run.call_count == 4
    
    # Verifica comandos específicos
    expected_commands = [
        ["dbt", "deps"],
        ["dbt", "debug"],
        ["dbt", "run"],
        ["dbt", "test"]
    ]
    
    for i, cmd in enumerate(expected_commands):
        assert mock_run.call_args_list[i][0][0] == cmd
    
    # Verifica logging
    assert mock_logger.info.call_count >= 4  # Um para cada comando + sucessos

@patch("src.utils.dbt_tools.subprocess.run")
@patch("src.utils.dbt_tools.logger")
def test_run_dbt_custom_project_dir(mock_logger, mock_run, monkeypatch):
    """Testa execução com diretório de projeto customizado via env var"""
    monkeypatch.setenv("DBT_PROJECT_DIR", "/custom/dbt/path")
    mock_run.return_value = MagicMock(stdout="OK", stderr="", returncode=0)
    
    run_dbt()
    
    # Verifica que cwd foi configurado corretamente
    assert mock_run.call_args[1]["cwd"] == "/custom/dbt/path"

@patch("src.utils.dbt_tools.subprocess.run")
@patch("src.utils.dbt_tools.logger")
def test_run_dbt_default_project_dir(mock_logger, mock_run):
    """Testa execução com diretório padrão quando env var não está setada"""
    # Garante que env var não está setada
    with patch.dict(os.environ, {}, clear=True):
        mock_run.return_value = MagicMock(stdout="OK", stderr="", returncode=0)
        
        run_dbt()
        
        # Verifica diretório padrão
        assert mock_run.call_args[1]["cwd"] == "/app/dbt"


# Testes de erro

@patch("src.utils.dbt_tools.subprocess.run")
@patch("src.utils.dbt_tools.logger")
def test_run_dbt_deps_fails(mock_logger, mock_run):
    """Testa falha no comando dbt deps (primeiro comando)"""
    error = subprocess.CalledProcessError(
        returncode=1,
        cmd=["dbt", "deps"],
        stderr="Package not found",
        output="Error output"
    )
    mock_run.side_effect = error
    
    with pytest.raises(RuntimeError, match="Erro no dbt: Package not found"):
        run_dbt()
    
    # Verifica que logou o erro
    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args
    assert "Falha na execução do dbt" in str(call_args)
    assert "Package not found" in str(call_args)

@patch("src.utils.dbt_tools.subprocess.run")
@patch("src.utils.dbt_tools.logger")
def test_run_dbt_run_fails(mock_logger, mock_run):
    """Testa falha no comando dbt run (terceiro comando)"""
    # Primeiros dois comandos sucedem, terceiro falha
    def side_effect(*args, **kwargs):
        if args[0] == ["dbt", "run"]:
            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=["dbt", "run"],
                stderr="Model compilation error",
                output=""
            )
        return MagicMock(stdout="OK", stderr="", returncode=0)
    
    mock_run.side_effect = side_effect
    
    with pytest.raises(RuntimeError, match="Erro no dbt: Model compilation error"):
        run_dbt()
    
    # Verifica que apenas 3 comandos foram tentados (deps, debug, run)
    assert mock_run.call_count == 3

@patch("src.utils.dbt_tools.subprocess.run")
@patch("src.utils.dbt_tools.logger")
def test_run_dbt_test_fails(mock_logger, mock_run):
    """Testa falha no comando dbt test (último comando)"""
    def side_effect(*args, **kwargs):
        if args[0] == ["dbt", "test"]:
            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=["dbt", "test"],
                stderr="Test failed: row_count > 0",
                output=""
            )
        return MagicMock(stdout="OK", stderr="", returncode=0)
    
    mock_run.side_effect = side_effect
    
    with pytest.raises(RuntimeError, match="Erro no dbt: Test failed"):
        run_dbt()
    
    # Todos os 4 comandos foram tentados
    assert mock_run.call_count == 4


# Testes de logging e output

@patch("src.utils.dbt_tools.subprocess.run")
@patch("src.utils.dbt_tools.logger")
def test_run_dbt_logs_summary_lines(mock_logger, mock_run):
    """Testa que linhas com 'Actual' e 'OK' são logadas em debug"""
    mock_run.return_value = MagicMock(
        stdout="Running with dbt=1.0.0\nActual: 100 rows\nOK created view\nFinished",
        stderr="",
        returncode=0
    )
    
    run_dbt()
    
    # Verifica que debug foi chamado com o output filtrado
    debug_calls = [call for call in mock_logger.debug.call_args_list 
                   if "dbt output" in str(call)]
    assert len(debug_calls) > 0

@patch("src.utils.dbt_tools.subprocess.run")
@patch("src.utils.dbt_tools.logger")
def test_run_dbt_empty_stdout(mock_logger, mock_run):
    """Testa comportamento quando stdout está vazio"""
    mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)
    
    run_dbt()
    
    # Não deve quebrar com stdout vazio
    # Verifica que não tentou logar debug (pois não há linhas para filtrar)
    debug_calls = [call for call in mock_logger.debug.call_args_list 
                   if "dbt output" in str(call)]
   