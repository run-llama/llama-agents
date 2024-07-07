import asyncio
import time
import numpy as np
import functools
from typing import Any, Callable


def exponential_delay(exponential_rate: float) -> Callable:
    """Wrapper for exponential tool."""

    def decorator(
        func: Callable,
    ) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> str:
            # random delay
            delay = np.random.exponential(exponential_rate)
            print(f"waiting for {delay} seconds", flush=True)
            time.sleep(delay)
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> str:
            # random delay
            delay = np.random.exponential(exponential_rate)
            print(f"waiting for {delay} seconds", flush=True)
            await asyncio.sleep(delay)
            return await func(*args, **kwargs)

        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper

    return decorator


async def main() -> None:
    @exponential_delay(2)
    async def get_the_secret_fact() -> str:
        """Returns the secret fact."""
        return "The secret fact is: A baby llama is called a 'Cria'."

    @exponential_delay(1)
    async def async_correct_first_character(input: str) -> str:
        """Corrects the first character."""
        tokens = input.split()
        return " ".join([t[-1] + t[0:-1] for t in tokens])

    output = await async_correct_first_character(input="eyh ouy")
    print(output)
    print(async_correct_first_character.__doc__)


if __name__ == "__main__":
    asyncio.run(main())
