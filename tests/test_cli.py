import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from freelancer_bot.app import cli, run_app
from freelancer_bot.config import RuntimeMode
from freelancer_bot.filters import FilterConfig
from freelancer_bot.replies import ReplyDraft
from freelancer_bot.sources import Source


class CliModeTest(unittest.TestCase):
    def test_default_cli_prints_help_without_starting_runtime(self):
        with (
            patch("sys.argv", ["freelancer_bot"]),
            patch("freelancer_bot.app.run_app", new_callable=AsyncMock) as run,
            redirect_stdout(io.StringIO()) as output,
        ):
            cli()

        run.assert_not_awaited()
        self.assertIn("--run", output.getvalue())

    def test_bot_only_is_explicit_and_uses_bot_only_mode(self):
        with (
            patch("sys.argv", ["freelancer_bot", "--bot-only"]),
            patch("freelancer_bot.app.run_app", new_callable=AsyncMock) as run,
        ):
            cli()

        run.assert_awaited_once_with(mode=RuntimeMode.BOT_ONLY)

    def test_run_is_explicit_and_uses_full_runtime_mode(self):
        with (
            patch("sys.argv", ["freelancer_bot", "--run"]),
            patch("freelancer_bot.app.run_app", new_callable=AsyncMock) as run,
        ):
            cli()

        run.assert_awaited_once_with(mode=RuntimeMode.RUN)

    def test_check_config_loads_config_without_runtime_credentials(self):
        config = SimpleNamespace(
            sources_path=Path("config/sources.json"),
            filters_path=Path("config/filters.json"),
        )
        source = Source("@test_source", "Test", "Fixture")
        filters = FilterConfig(5, {"telegram": 5}, ("smm",))

        with (
            patch("sys.argv", ["freelancer_bot", "--check-config"]),
            patch("freelancer_bot.app.RuntimeConfig.from_env", return_value=config) as from_env,
            patch("freelancer_bot.app.load_sources", return_value=[source]),
            patch("freelancer_bot.app.load_filter_config", return_value=filters),
            redirect_stdout(io.StringIO()),
        ):
            cli()

        from_env.assert_called_once_with(mode=RuntimeMode.CHECK_CONFIG)

    def test_check_filter_uses_check_filter_mode(self):
        config = SimpleNamespace(filters_path=Path("config/filters.json"))
        filters = FilterConfig(5, {"telegram": 5}, ())

        with (
            patch("sys.argv", ["freelancer_bot", "--check-filter", "telegram"]),
            patch("freelancer_bot.app.RuntimeConfig.from_env", return_value=config) as from_env,
            patch("freelancer_bot.app.load_filter_config", return_value=filters),
            redirect_stdout(io.StringIO()),
        ):
            cli()

        from_env.assert_called_once_with(mode=RuntimeMode.CHECK_FILTER)

    def test_check_sources_uses_user_only_mode(self):
        config = SimpleNamespace()
        source_check = AsyncMock()

        with (
            patch("sys.argv", ["freelancer_bot", "--check-sources"]),
            patch("freelancer_bot.app.RuntimeConfig.from_env", return_value=config) as from_env,
            patch("freelancer_bot.app.check_sources", source_check),
        ):
            cli()

        from_env.assert_called_once_with(mode=RuntimeMode.CHECK_SOURCES)
        source_check.assert_awaited_once_with(config)

    def test_draft_text_uses_ai_only_mode(self):
        config = SimpleNamespace(
            freelancer_profile_path=Path("profile.json"),
            filters_path=Path("filters.json"),
        )
        filters = FilterConfig(5, {"telegram": 5}, ())
        provider = MagicMock()
        provider.generate.return_value = ReplyDraft(
            fit_summary="Подходит",
            fit_score=80,
            risks=(),
            questions_to_client=(),
            proposal_draft="Могу помочь.",
            short_reply="Могу помочь.",
        )

        with (
            patch("sys.argv", ["freelancer_bot", "--draft-text", "Нужен telegram бот"]),
            patch("freelancer_bot.app.RuntimeConfig.from_env", return_value=config) as from_env,
            patch("freelancer_bot.app.load_freelancer_profile", return_value={}),
            patch("freelancer_bot.app.load_filter_config", return_value=filters),
            patch("freelancer_bot.app._build_reply_draft_provider", return_value=provider),
            redirect_stdout(io.StringIO()),
        ):
            cli()

        from_env.assert_called_once_with(mode=RuntimeMode.DRAFT_TEXT)
        provider.generate.assert_called_once()


class RunAppModeTest(unittest.IsolatedAsyncioTestCase):
    async def test_normal_run_uses_full_runtime_mode(self):
        config = SimpleNamespace(log_level="INFO")
        application = MagicMock()
        application.run = AsyncMock()
        application.shutdown = AsyncMock()

        with (
            patch("freelancer_bot.app.RuntimeConfig.from_env", return_value=config) as from_env,
            patch("freelancer_bot.app.Redactor.from_config"),
            patch("freelancer_bot.app.configure_structured_logger"),
            patch("freelancer_bot.app.LeadBot", return_value=application),
        ):
            await run_app()

        from_env.assert_called_once_with(mode=RuntimeMode.RUN)
        application.run.assert_awaited_once()
        application.shutdown.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
