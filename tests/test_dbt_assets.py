"""Tests for the dbt asset translator logic."""

from qmi_etl.defs.dbt_assets import BqDbtTranslator


class TestBqDbtTranslator:
    def setup_method(self):
        self.translator = BqDbtTranslator()

    def test_seed_resource_type(self):
        props = {"resource_type": "seed", "fqn": ["project", "my_seed"]}
        assert self.translator.get_group_name(props) == "seeds_dbt"

    def test_base_folder(self):
        props = {"resource_type": "model", "fqn": ["project", "base", "model_a"]}
        assert self.translator.get_group_name(props) == "base_dbt"

    def test_staging_folder(self):
        props = {"resource_type": "model", "fqn": ["project", "staging", "stg_orders"]}
        assert self.translator.get_group_name(props) == "staging_dbt"

    def test_reporting_folder(self):
        props = {"resource_type": "model", "fqn": ["project", "reporting", "rpt_sales"]}
        assert self.translator.get_group_name(props) == "reporting_dbt"

    def test_unknown_folder_gets_dbt_suffix(self):
        props = {"resource_type": "model", "fqn": ["project", "custom", "my_model"]}
        assert self.translator.get_group_name(props) == "custom_dbt"

    def test_short_fqn_returns_default(self):
        props = {"resource_type": "model", "fqn": ["project"]}
        assert self.translator.get_group_name(props) == "bq_dbt"

    def test_missing_fqn_returns_default(self):
        props = {"resource_type": "model"}
        assert self.translator.get_group_name(props) == "bq_dbt"

    def test_seed_without_fqn(self):
        props = {"resource_type": "seed"}
        assert self.translator.get_group_name(props) == "seeds_dbt"
