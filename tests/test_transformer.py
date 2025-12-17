"""Tests pour le transformer."""
import pytest
import pandas as pd
from pipeline.transformer import DataTransformer


class TestDataTransformer:
    
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            'code': ['001', '002', '001', '003'],
            'name': ['  Test  ', None, 'Test', 'Other'],
            'value': [10.0, None, 10.0, 100.0],
        })
    
    def test_remove_duplicates(self, sample_df):
        transformer = DataTransformer(sample_df)
        result = transformer.remove_duplicates(['code']).get_result()
        assert len(result) == 3
        assert result['code'].nunique() == 3
    
    def test_handle_missing_values_median(self, sample_df):
        transformer = DataTransformer(sample_df)
        result = transformer.handle_missing_values(numeric_strategy='median').get_result()
        assert result['value'].isnull().sum() == 0
    
    def test_normalize_text(self, sample_df):
        transformer = DataTransformer(sample_df)
        result = transformer.normalize_text_columns(['name']).get_result()
        assert 'test' in result['name'].values
    
    def test_chaining(self, sample_df):
        transformer = DataTransformer(sample_df)
        result = (
            transformer
            .remove_duplicates()
            .handle_missing_values()
            .get_result()
        )
        assert len(transformer.transformations_applied) >= 2
