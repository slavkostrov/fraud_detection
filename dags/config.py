from dataclasses import dataclass


@dataclass
class BaseETLConfig:
    n_customers: int = 5_000
    n_terminals: int = 5_000

    output_prefix: str = "/user/ubuntu/practice_1/"

    radius: float = 3
    num_val_days: int = 3

