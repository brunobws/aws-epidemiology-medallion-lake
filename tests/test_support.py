import pytest
from unittest.mock import Mock, patch
from aws.modules.support import (
    summarize_exception,
    get_date_and_time,
    split_target_table,
    eval_values,
    write_error_logs,
)

class TestSummarizeException:
    def test_returns_empty_string_for_none(self):
        assert summarize_exception(None) == ""

    def test_returns_empty_string_for_empty_file_sentinel(self):
        assert summarize_exception(Exception("empty_file")) == ""

    def test_empty_file_sentinel_is_case_insensitive(self):
        assert summarize_exception(Exception("EMPTY_FILE")) == ""
        assert summarize_exception(Exception("Empty_File")) == ""

    def test_returns_structured_string_for_python_exception(self):
        try:
            1 / 0
        except Exception as e:
            res = summarize_exception(e)
            assert "ZeroDivisionError" in res
            assert "division by zero" in res

    def test_includes_line_number_in_result(self):
        try:
            raise ValueError("Test error")
        except Exception as e:
            res = summarize_exception(e)
            assert "line_number" in res

    def test_different_exception_types_are_reflected(self):
        try:
            {}["missing_key"]
        except Exception as e:
            res = summarize_exception(e)
            assert "KeyError" in res


class TestGetDateAndTime:
    def test_returns_string(self):
        assert isinstance(get_date_and_time(), str)

    def test_format_is_correct(self):
        # Format should match YYYY-MM-DD HH:MM:SS
        import re
        assert re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", get_date_and_time())


class TestSplitTargetTable:
    def test_standard_two_segment_table(self):
        assert split_target_table("breweries_tb_breweries") == ("tb_breweries", "breweries")

    def test_multi_segment_table_name(self):
        assert split_target_table("src_tb_fact_sales") == ("tb_fact_sales", "src")

    def test_source_is_always_first_segment(self):
        _, source = split_target_table("api_data_dump_raw")
        assert source == "api"

    def test_table_name_joins_remaining_segments(self):
        table_name, _ = split_target_table("api_data_dump_raw")
        assert table_name == "data_dump_raw"


class TestEvalValues:
    def test_true_string_returns_bool_true(self):
        assert eval_values("true") is True

    def test_false_string_returns_bool_false(self):
        assert eval_values("false") is False

    def test_boolean_strings_are_case_insensitive(self):
        assert eval_values("True") is True
        assert eval_values("FALSE") is False

    def test_dict_string_returns_dict(self):
        assert eval_values('{"key": "value"}') == {"key": "value"}

    def test_list_string_returns_list(self):
        assert eval_values("[1, 2, 3]") == [1, 2, 3]

    def test_integer_string_returns_int(self):
        assert eval_values("42") == 42

    def test_non_string_passthrough(self):
        assert eval_values(42) == 42
        assert eval_values(3.14) == 3.14
        assert eval_values({"a": 1}) == {"a": 1}

    def test_none_returns_none(self):
        assert eval_values(None) is None

    def test_empty_string_returns_empty_string(self):
        assert eval_values("") == ""

    def test_invalid_string_raises_exception(self):
        with pytest.raises(Exception, match="Parsing error"):
            eval_values("{invalid_python_syntax!")


class TestWriteErrorLogs:
    def _get_exception(self, msg="test"):
        try:
            raise Exception(msg)
        except Exception as e:
            return e

    def test_always_raises_exception(self):
        with pytest.raises(Exception):
            write_error_logs(logger=None, error_msg="Test", e=self._get_exception())

    def test_exception_message_contains_error_msg(self):
        with pytest.raises(Exception, match="Custom error message"):
            write_error_logs(logger=None, error_msg="Custom error message", e=self._get_exception())

    def test_calls_logger_error_when_logger_is_provided(self):
        mock_logger = Mock()
        with pytest.raises(Exception):
            write_error_logs(logger=mock_logger, error_msg="Test", e=self._get_exception())
        mock_logger.error.assert_called_once()

    def test_sends_email_when_destination_is_set(self):
        mock_super = Mock()
        with pytest.raises(Exception):
            write_error_logs(
                logger=None,
                error_msg="Test",
                e=self._get_exception(),
                destination=["test@example.com"],
                super=mock_super,
            )
        mock_super.send_email_on_failure.assert_called_once()

    def test_skips_email_for_empty_file_sentinel(self):
        mock_super = Mock()
        with pytest.raises(Exception):
            write_error_logs(
                logger=None,
                error_msg="Test",
                e=self._get_exception("empty_file"),
                destination=["test@example.com"],
                super=mock_super,
            )
        mock_super.send_email_on_failure.assert_not_called()
