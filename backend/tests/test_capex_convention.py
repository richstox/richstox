"""
P7 Regression Test: CapEx Sign Convention

Purpose: Ensure the FCF calculation uses abs(capex) to handle 
inconsistent sign conventions in EODHD data.

If this test fails, the sign convention has been silently changed,
which would break FCF Yield calculations.

Run: cd /app/backend && python -m pytest tests/test_capex_convention.py -v
"""

import pytest
import re


class TestCapExSignConvention:
    """
    P7 CRITICAL: FCF formula must use abs(capex) to normalize 
    inconsistent EODHD data.
    """
    
    @pytest.fixture
    def fcf_code(self):
        """Load the FCF calculation code from local_metrics_service.py"""
        with open('/app/backend/local_metrics_service.py', 'r') as f:
            return f.read()
    
    def test_fcf_uses_abs_capex(self, fcf_code):
        """
        CRITICAL: Code must use abs(capex) when calculating FCF.
        
        Rationale: EODHD stores capitalExpenditures inconsistently
        (sometimes negative, sometimes positive). Using abs() ensures
        CapEx is always treated as a cash outflow.
        
        Expected formula: FCF = OCF - abs(CapEx)
        """
        # Look for the pattern: abs(capex)
        assert 'abs(capex)' in fcf_code, \
            "FCF calculation MUST use abs(capex) to normalize sign convention"
    
    def test_fcf_subtracts_capex_from_ocf(self, fcf_code):
        """
        CRITICAL: FCF must SUBTRACT capex from operating cash flow.
        
        Pattern: operating_cf - abs(capex)
        """
        # Find the FCF calculation line
        fcf_pattern = r'operating_cf\s*-\s*abs\(capex\)'
        match = re.search(fcf_pattern, fcf_code)
        
        assert match is not None, \
            "FCF formula must be: operating_cf - abs(capex)"
    
    def test_capex_null_handling(self, fcf_code):
        """
        When CapEx is null/missing, FCF should equal OCF.
        
        Pattern: 'if capex else operating_cf' or similar
        """
        # The code should handle null capex
        has_null_handling = (
            'if capex else' in fcf_code or
            'capex else operating_cf' in fcf_code or
            'if capex' in fcf_code
        )
        
        assert has_null_handling, \
            "Code must handle null CapEx (FCF = OCF when CapEx missing)"
    
    def test_documentation_matches_code(self):
        """
        METRIC_DEFINITIONS.md must document the abs(capex) convention.
        """
        with open('/app/docs/METRIC_DEFINITIONS.md', 'r') as f:
            doc = f.read()
        
        # Doc must mention abs() convention
        assert 'abs(capex)' in doc.lower() or 'abs(CapEx)' in doc or '|CapEx|' in doc, \
            "Documentation must explicitly state abs(capex) convention"
        
        # Doc must explain why (EODHD inconsistency)
        assert 'inconsistent' in doc.lower() or 'normalize' in doc.lower(), \
            "Documentation must explain why abs() is needed"


class TestFCFFormulaIntegrity:
    """
    Additional tests to ensure FCF formula hasn't been accidentally changed.
    """
    
    @pytest.fixture
    def fcf_code(self):
        with open('/app/backend/local_metrics_service.py', 'r') as f:
            return f.read()
    
    def test_fcf_yield_formula(self, fcf_code):
        """
        FCF Yield = (total_fcf / market_cap) * 100
        """
        assert 'total_fcf / market_cap' in fcf_code or \
               'total_fcf/market_cap' in fcf_code, \
            "FCF Yield formula must be: total_fcf / market_cap"
    
    def test_fcf_yield_is_percentage(self, fcf_code):
        """
        FCF Yield must be multiplied by 100 to get percentage.
        """
        # Pattern: (something / market_cap) * 100
        pattern = r'\(.*?/\s*market_cap\s*\)\s*\*\s*100'
        assert re.search(pattern, fcf_code), \
            "FCF Yield must be converted to percentage (* 100)"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
