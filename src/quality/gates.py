"""Quality Gate - Decision maker for pass/fail."""

import logging
from dataclasses import dataclass

from .validators import ValidationResult, ValidationConfig

logger = logging.getLogger(__name__)


class ValidationHardFailError(Exception):
    """Raised when validation fails hard."""
    pass


@dataclass
class GateResult:
    """Quality gate result."""
    status: str  # 'success', 'warning', 'failed'
    valid_rate: float
    message: str


class QualityGate:
    """Decision maker for pass/fail based on validation results."""
    
    def __init__(self, config: ValidationConfig = None):
        self.config = config or ValidationConfig()
    
    def evaluate(self, result: ValidationResult) -> GateResult:
        """Evaluate validation result. Raises ValidationHardFailError on hard fail."""
        
        # Hard fail: No jobs or below minimum
        if result.total_jobs == 0:
            raise ValidationHardFailError('No jobs found')
        
        if result.total_jobs < self.config.min_job_count:
            raise ValidationHardFailError(f'Job count {result.total_jobs} below minimum {self.config.min_job_count}')
        
        if result.duplicate_rate > self.config.hard_fail_duplicate_rate:
            raise ValidationHardFailError(f'Duplicate rate {result.duplicate_rate:.1%} too high')
        
        # Staging-specific: Check data loss
        if result.data_loss_rate is not None and result.data_loss_rate > 0.05:
            raise ValidationHardFailError(f'Data loss {result.data_loss_rate:.1%} exceeds 5%')
        
        # Hard fail: Low valid rate
        if result.valid_rate < self.config.warning_threshold:
            raise ValidationHardFailError(f'Valid rate {result.valid_rate:.1%} below threshold')
        
        # Warning
        if result.valid_rate < self.config.success_threshold:
            logger.warning(f'Valid rate {result.valid_rate:.1%} - warning')
            return GateResult('warning', result.valid_rate, f'Warning: {result.valid_rate:.1%} valid')
        
        # Success
        logger.info(f'Validation passed: {result.valid_rate:.1%} valid')
        return GateResult('success', result.valid_rate, f'Passed: {result.valid_rate:.1%} valid')
