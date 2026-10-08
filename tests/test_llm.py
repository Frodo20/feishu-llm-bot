from pathlib import Path


def test_retired_module_has_no_model_client() -> None:
    module = Path(__file__).parents[1] / "src" / "feishu_llm_bot" / "llm.py"
    source = module.read_text(encoding="utf-8")
    assert "import anthropic" not in source
    assert "class LLMClient" not in source
    assert ".messages.create" not in source
