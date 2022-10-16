from dataclasses import dataclass


@dataclass
class BaseETLConfig:
    n_customers: int = 5000
    n_terminals: int = 5000

    output_prefix: str = "/user/ubuntu/practice_1/"

    radius: float = 3
